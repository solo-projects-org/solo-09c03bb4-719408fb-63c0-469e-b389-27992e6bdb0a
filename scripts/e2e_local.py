#!/usr/bin/env python3
"""本地真实 PostgreSQL 的端到端恢复验证（不依赖 Docker）。

用法（在开发机上）：
    PGBIN=/path/to/postgresql/bin python3 scripts/e2e_local.py

要求：
- 本机可用的 postgres / initdb / pg_ctl / pg_basebackup；
- 能用当前用户启动一个临时 scratch 实例；
- 本脚本在临时目录里完成：源库 → 备份 → 核对点 → 恢复 → 查询。

它复用 app 的 config / backuper / recoverer，环境变量按临时目录设置。
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))

PGBIN = os.environ.get("PGBIN", "")
if PGBIN:
    os.environ["PATH"] = f"{PGBIN}{os.pathsep}" + os.environ.get("PATH", "")

WORK = Path(tempfile.mkdtemp(prefix="pitr-e2e-"))
SRC = WORK / "src"
ARCHIVE = WORK / "wal_archive"
BACKUPS = WORK / "backups"
RECOVERIES = WORK / "recoveries"
STATE = WORK / "state" / "api.db"
SOCK = WORK / "sock"
for d in (ARCHIVE, BACKUPS, RECOVERIES, STATE.parent, SOCK):
    d.mkdir(parents=True, exist_ok=True)

os.environ.update(
    SOURCE_HOST="127.0.0.1",
    SOURCE_PORT="55439",
    SOURCE_DB="water",
    SOURCE_USER=os.environ.get("USER", "node"),
    SOURCE_PASSWORD="",
    ARCHIVE_DIR=str(ARCHIVE),
    BACKUP_DIR=str(BACKUPS),
    RECOVERY_DIR=str(RECOVERIES),
    STATE_DB=str(STATE),
    RECOVERY_PORT="55441",
    RECOVERY_SOCKET_DIR=str(SOCK),
)

FAILS: list[str] = []


def check(label, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def psql(sql, db="water", port=55439, quiet=False):
    r = run(["psql", "-h", str(SOCK), "-p", str(port), "-U",
             os.environ["SOURCE_USER"], "-d", db, "-tA", "-v",
             "ON_ERROR_STOP=1", "-c", sql])
    if r.returncode != 0:
        raise RuntimeError(f"psql 失败: {sql}\n{r.stderr}")
    return r.stdout.strip()


def wait(fn, desc, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if fn():
            return
        time.sleep(0.5)
    raise RuntimeError(f"超时：{desc}")


def main():
    from app import backuper, config, recoverer, source, state

    # ---------------- 源库
    r = run(["initdb", "-D", str(SRC), "--no-sync", "-A", "trust"])
    assert r.returncode == 0, r.stderr
    with open(SRC / "postgresql.conf", "a") as f:
        f.write(f"""
port = 55439
listen_addresses = '127.0.0.1'
unix_socket_directories = '{SOCK}'
archive_mode = on
wal_level = replica
archive_command = 'cp %p {ARCHIVE}/.%f.tmp && mv {ARCHIVE}/.%f.tmp {ARCHIVE}/%f'
""")
    r = run(["pg_ctl", "-D", str(SRC), "-l", str(WORK / "src.log"),
             "-w", "-t", "60", "start"])
    assert r.returncode == 0, r.stderr
    user = os.environ["SOURCE_USER"]
    psql("CREATE DATABASE water", db="postgres")
    with open(ROOT / "db" / "init" / "01_schema.sql") as f:
        psql(f.read())
    print("源库就绪：", psql("SELECT count(*) FROM samples"))

    # ---------------- 初次备份
    state.init_db()
    bid = backuper.start_backup()
    wait(lambda: (state.one_row("SELECT * FROM backups WHERE id=?", (bid,))
                  or {}).get("status") != "running", "初次备份")
    row = state.one_row("SELECT * FROM backups WHERE id=?", (bid,))
    check("初次基准备份发布成功", row["status"] == "done")
    check("备份目录唯一且带发布标记",
          os.path.isdir(row["path"])
          and os.path.isfile(os.path.join(row["path"], backuper.PUBLISH_MARKER)))
    first_backup = row

    # ---------------- cp1 → 误删 → cp2
    lsn1, wal1 = source.create_restore_point("cp1")
    state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)", ("cp1", lsn1, wal1, "waiting_wal", state.now()))
    psql("DELETE FROM samples WHERE sample_no='S-2026-0003'")
    psql("UPDATE samples SET ph=7.55 WHERE sample_no='S-2026-0002'")
    lsn2, wal2 = source.create_restore_point("cp2")
    state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)", ("cp2", lsn2, wal2, "waiting_wal", state.now()))
    from app import checkpoints as cpmod
    wait(lambda: (cpmod.refresh(backuper.latest_usable()) is not None)
         and state.one_row("SELECT status FROM checkpoints WHERE name='cp1'")["status"]
         == "ready", "cp1 归档")
    check("cp1 ready", state.one_row(
        "SELECT status FROM checkpoints WHERE name='cp1'")["status"] == "ready")
    cpmod.refresh(backuper.latest_usable())
    check("cp2 ready", state.one_row(
        "SELECT status FROM checkpoints WHERE name='cp2'")["status"] == "ready")

    # ---------------- 恢复 cp1（旧备份）
    def restore(name):
        jid = state.execute(
            "INSERT INTO jobs (checkpoint_name, status, created_at, updated_at)"
            " VALUES (?,?,?,?)", (name, "queued", state.now(), state.now()))
        recoverer.run_job(jid)
        return state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))

    job = restore("cp1")
    check("cp1 恢复 ready", job["status"] == "ready")
    if job["status"] != "ready":
        print(job.get("error"), job.get("log_tail"))
        return 1
    rows = recoverer.fetch_samples(None, None, 100)
    by_no = {r["sample_no"]: r for r in rows}
    check("cp1 中误删的 S-2026-0003 回来了", "S-2026-0003" in by_no)
    check("cp1 中 S-2026-0002 是删改前 pH 6.95",
          by_no.get("S-2026-0002", {}).get("ph") == 6.95)
    try:
        subprocess.run(
            ["psql", "-h", str(SOCK), "-p", "55441", "-U", user, "-d", "water",
             "-c", "INSERT INTO samples(sample_no,station_no,ph)"
             " VALUES ('X','X',1)"], capture_output=True, text=True, check=True)
        check("恢复实例拒绝写入", False)
    except subprocess.CalledProcessError:
        check("恢复实例拒绝写入", True)

    # ---------------- 重做备份：失败必须保留旧备份
    real = backuper._pg_basebackup
    backuper._pg_basebackup = lambda t: (_ for _ in ()).throw(
        RuntimeError("模拟备份失败"))
    bid2 = backuper.start_backup(force=True)
    wait(lambda: state.one_row("SELECT status FROM backups WHERE id=?",
                               (bid2,))["status"] != "running", "失败备份")
    check("重做失败如实标记 failed",
          state.one_row("SELECT status FROM backups WHERE id=?", (bid2,))["status"]
          == "failed")
    usable = backuper.latest_usable()
    check("失败后仍选用原备份", usable["id"] == first_backup["id"])
    check("旧备份目录仍在", os.path.isdir(first_backup["path"]))

    # ---------------- cp1 仍可用原备份恢复（重建任务）
    job2 = restore("cp1")
    check("失败后按原核对点再次恢复成功", job2["status"] == "ready")
    if job2["status"] == "ready":
        rows = recoverer.fetch_samples(None, None, 100)
        check("再次恢复仍含删前样品",
              "S-2026-0003" in {r["sample_no"] for r in rows})
    else:
        print(job2.get("error"), job2.get("log_tail"))

    # ---------------- 崩溃在登记前：模拟 running 行 + 已 rename 目录
    backuper._pg_basebackup = real
    orphan = BACKUPS / "base-20260101T000000Z-crash"
    shutil.copytree(first_backup["path"], orphan)
    (orphan / backuper.PUBLISH_MARKER).write_text(json.dumps(
        {"backup_id": 999, "start_lsn": "0/1"}))
    interrupted = state.execute(
        "INSERT INTO backups (path, status, created_at) VALUES (?,?,?)",
        (str(orphan), "running", state.now()))
    rep = backuper.reconcile()
    check("重启收敛 running → failed", interrupted in rep["interrupted"])
    check("未登记目录被清理", not orphan.exists())
    check("旧备份仍可用", backuper.latest_usable()["id"] == first_backup["id"])

    # ---------------- 成功重做 → 更早核对点按新起点过期
    lsn3, wal3 = source.create_restore_point("cp3")
    state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)", ("cp3", lsn3, wal3, "waiting_wal", state.now()))
    cpmod.refresh(backuper.latest_usable())
    bid3 = backuper.start_backup()   # 上一次 failed，无需 force
    wait(lambda: state.one_row("SELECT status FROM backups WHERE id=?",
                               (bid3,))["status"] != "running", "重做备份")
    row3 = state.one_row("SELECT * FROM backups WHERE id=?", (bid3,))
    check("重做备份成功", row3["status"] == "done")
    check("新旧备份目录并存",
          os.path.isdir(first_backup["path"]) and os.path.isdir(row3["path"]))
    from app.walutil import lsn_to_int
    cp1_status = state.one_row(
        "SELECT status FROM checkpoints WHERE name='cp1'")["status"]
    expect = ("expired" if lsn_to_int(lsn1) < lsn_to_int(row3["start_lsn"])
              else "ready")
    check(f"cp1 按新备份起点判定为 {expect}（实际 {cp1_status}）",
          cp1_status == expect)
    check("cp3（早于新备份起点）已过期",
          state.one_row("SELECT status FROM checkpoints WHERE name='cp3'")[
              "status"] == "expired")

    # 新备份完成后再登记的核对点仍可用新备份恢复
    lsn4, wal4 = source.create_restore_point("cp_after")
    state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)", ("cp_after", lsn4, wal4, "waiting_wal", state.now()))
    wait(lambda: (cpmod.refresh(backuper.latest_usable()) is not None) and
         state.one_row("SELECT status FROM checkpoints WHERE name='cp_after'")[
             "status"] == "ready", "cp_after 归档")
    job3 = restore("cp_after")
    check("cp_after 按新备份恢复成功", job3["status"] == "ready")
    if job3["status"] != "ready":
        print(job3.get("error"))
        print(job3.get("log_tail"))

    print()
    if FAILS:
        print(f"失败 {len(FAILS)} 项：")
        for f in FAILS:
            print(" -", f)
        return 1
    print("E2E 全部通过 ✅  工作目录：", WORK)
    return 0


if __name__ == "__main__":
    try:
        code = main()
    finally:
        # 停掉本脚本拉起的恢复实例，避免端口残留影响下一次运行
        for job in os.listdir(RECOVERIES) if RECOVERIES.exists() else []:
            ddir = RECOVERIES / job
            if (ddir / "postmaster.pid").exists():
                subprocess.run(["pg_ctl", "-D", str(ddir), "-m", "immediate",
                                "stop"], capture_output=True)
        subprocess.run(["pg_ctl", "-D", str(SRC), "-m", "immediate", "stop"],
                       capture_output=True)
    sys.exit(code)
