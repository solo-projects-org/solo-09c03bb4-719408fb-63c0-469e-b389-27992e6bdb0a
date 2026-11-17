"""备份发布 / 重启衔接的回归测试（不依赖真实 PostgreSQL）。

覆盖场景：
- 发布失败（pg_basebackup 失败、目录重命名失败、登记前崩溃）→ 旧备份继续可用，
  失败/未登记的新备份绝不被选中；
- API 重启 → running 备份收敛为 failed、未登记目录被清理、旧 done 备份不受影响；
- "显示完成却指向新数据" → done 行目录内容/登记 LSN 不一致时不被选用；
- 发布成功 → 仅在登记 done 之后才按新起点把更早核对点标记过期。
"""
import json
import os
import threading
import time
import types

import pytest

from app import backuper, checkpoints, config, state
from app.walutil import lsn_to_int


# ------------------------------------------------------------------ 夹具

@pytest.fixture()
def env(tmp_path, monkeypatch):
    """隔离的备份目录 / 恢复目录 / 状态库，并重置全局锁。"""
    backup_dir = tmp_path / "backups"
    recovery_dir = tmp_path / "recoveries"
    state_db = tmp_path / "state" / "api.db"
    for d in (backup_dir, recovery_dir, state_db.parent):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(config, "RECOVERY_DIR", str(recovery_dir))
    monkeypatch.setattr(config, "STATE_DB", str(state_db))
    monkeypatch.setattr(config, "ARCHIVE_DIR", str(tmp_path / "wal_archive"))
    monkeypatch.setattr(backuper, "_lock", threading.Lock())
    state.init_db()
    return types.SimpleNamespace(
        tmp=tmp_path, backup_dir=backup_dir, recovery_dir=recovery_dir)


def make_manifest_backup(path, start_lsn: str, timeline: int = 1) -> None:
    """构造一个最小但可被校验的 pg_basebackup 目录。"""
    path = str(path)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "backup_manifest"), "w") as f:
        json.dump({"WAL-Ranges": [
            {"Timeline": timeline, "Start-LSN": start_lsn}]}, f)
    with open(os.path.join(path, "PG_VERSION"), "w") as f:
        f.write("16\n")


def insert_backup(start_lsn: str | None, path: str | None,
                  status: str = "done") -> int:
    return state.execute(
        "INSERT INTO backups (path, status, start_lsn, timeline, created_at)"
        " VALUES (?,?,?,?,?)",
        (path, status, start_lsn, 1, state.now()),
    )


def insert_checkpoint(name: str, lsn: str, status: str = "ready") -> int:
    return state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)",
        (name, lsn, "000000010000000000000001", status, state.now()),
    )


def wait_finished(bid: int, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = state.one_row("SELECT * FROM backups WHERE id=?", (bid,))
        if row["status"] != "running":
            return row
        time.sleep(0.02)
    raise AssertionError(f"备份 #{bid} 未在 {timeout}s 内结束")


def wait_status(table: str, column: str, value: str, expected: str,
                timeout: float = 5.0) -> str:
    """等待登记后的异步收尾（过期判定）完成（发布提交点之后执行）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = state.one_row(
            f"SELECT status FROM {table} WHERE {column}=?", (value,))["status"]
        if status == expected:
            return status
        time.sleep(0.02)
    return status


# ------------------------------------------------------------------ 发布失败

def test_failed_backup_keeps_old_usable(env, monkeypatch):
    """pg_basebackup 失败：旧备份仍是可用选择，失败行不被选中。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)

    def boom(_target):
        raise RuntimeError("pg_basebackup 崩了")

    monkeypatch.setattr(backuper, "_pg_basebackup", boom)
    bid = backuper.start_backup(force=True)
    row = wait_finished(bid)

    assert row["status"] == "failed"
    assert "pg_basebackup" in (row["error"] or "")
    assert backuper.latest_usable()["id"] == old_id
    assert not os.path.isdir(row["path"])


def test_publish_rename_failure_keeps_old_usable(env, monkeypatch):
    """staging 已成功下载但原子发布失败：旧备份不受影响，staging 被清理。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)

    def ok_backup(target):
        make_manifest_backup(target, "0/9000000")

    monkeypatch.setattr(backuper, "_pg_basebackup", ok_backup)
    real_rename = os.rename

    def failing_rename(src, dst):
        if os.path.basename(dst).startswith("base-") and ".staging-" in str(src):
            raise OSError("模拟 rename 失败")
        return real_rename(src, dst)

    monkeypatch.setattr(backuper.os, "rename", failing_rename)
    bid = backuper.start_backup(force=True)
    row = wait_finished(bid)

    assert row["status"] == "failed"
    assert backuper.latest_usable()["id"] == old_id
    assert [n for n in os.listdir(env.backup_dir) if n.startswith(".staging-")] == []
    assert os.path.isdir(old)


def test_registration_failure_does_not_publish(env, monkeypatch):
    """目录 rename 成功、但登记 done 的 SQLite 写失败：该备份不能被选中。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)

    def ok_backup(target):
        make_manifest_backup(target, "0/9000000")

    monkeypatch.setattr(backuper, "_pg_basebackup", ok_backup)
    real_execute = state.execute
    tripped = {"done": False}

    def flaky_execute(sql, params=()):
        if (not tripped["done"] and sql.lstrip().startswith("UPDATE backups")
                and "status='done'" in sql):
            tripped["done"] = True
            raise RuntimeError("模拟登记时进程/磁盘故障")
        return real_execute(sql, params)

    monkeypatch.setattr(backuper.state, "execute", flaky_execute)
    bid = backuper.start_backup(force=True)
    row = wait_finished(bid)

    assert tripped["done"] is True
    assert row["status"] == "failed"
    # 失败登记后的目录清理也异步收尾，短暂等待避免竞态
    for _ in range(100):
        if not os.path.isdir(row["path"]):
            break
        time.sleep(0.02)
    assert not os.path.isdir(row["path"])          # 未登记目录已清理
    assert backuper.latest_usable()["id"] == old_id


def test_crash_before_registration_is_not_selectable(env):
    """模拟发布目录已 rename、但进程在登记 done 前崩溃。

    running 行必须被重启收敛为 failed，目录清理；旧备份继续可用。
    """
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)

    orphan = os.path.join(env.backup_dir, "base-20260101T000000Z-deadbeef")
    make_manifest_backup(orphan, "0/9000000")
    with open(os.path.join(orphan, backuper.PUBLISH_MARKER), "w") as f:
        json.dump({"backup_id": 999, "start_lsn": "0/9000000",
                   "published_at": state.now()}, f)
    interrupted = insert_backup(None, orphan, status="running")

    report = backuper.reconcile()
    assert interrupted in report["interrupted"]
    row = state.one_row("SELECT * FROM backups WHERE id=?", (interrupted,))
    assert row["status"] == "failed"
    assert not os.path.isdir(orphan)
    assert backuper.latest_usable()["id"] == old_id


def test_restart_after_failure_old_checkpoint_still_restorable(env):
    """重启后 running→failed，旧 done 备份仍可供旧核对点恢复。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)
    cp_id = insert_checkpoint("cp-old", "0/5000000")

    new = os.path.join(env.backup_dir, "base-new")
    make_manifest_backup(new, "0/9000000")
    running = insert_backup(None, new, status="running")

    backuper.reconcile()
    # 未发布备份不得使核对点过期
    checkpoints.expire_before(backuper.latest_usable())
    cp = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cp_id,))
    assert cp["status"] == "ready"
    # 仍按原备份恢复
    usable = backuper.latest_usable()
    assert usable["id"] == old_id
    assert lsn_to_int(cp["restore_lsn"]) >= lsn_to_int(usable["start_lsn"])
    assert state.one_row("SELECT status FROM backups WHERE id=?",
                         (running,))["status"] == "failed"


# ------------------------------------------------------------------ 内容校验

def test_done_but_missing_directory_is_not_usable(env):
    """done 行目录失联（旧逻辑移走后失联）→ 回退到旧备份，绝不选中。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)
    gone = os.path.join(env.backup_dir, "base-gone")
    gone_id = insert_backup("0/9000000", gone)  # 目录不存在

    assert backuper.latest_usable()["id"] == old_id
    report = backuper.reconcile()
    assert gone_id in report["invalid_done"]
    assert state.one_row("SELECT status FROM backups WHERE id=?",
                         (gone_id,))["status"] == "failed"
    assert backuper.latest_usable()["id"] == old_id


def test_done_pointing_to_new_data_is_rejected(env):
    """done 行路径被替换成另一份备份（LSN 不符）→ 不选用。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)

    swapped = os.path.join(env.backup_dir, "base-swapped")
    make_manifest_backup(swapped, "0/9000000")  # 实际是新数据
    bad_id = insert_backup("0/2000000", swapped)

    assert not backuper.is_published(
        state.one_row("SELECT * FROM backups WHERE id=?", (bad_id,)))
    assert backuper.latest_usable()["id"] == old_id


def test_marker_mismatch_rejected(env):
    """发布标记与登记行不一致（目录是另一行发布的）→ 不选用。"""
    path = os.path.join(env.backup_dir, "base-x")
    make_manifest_backup(path, "0/2000000")
    bid = insert_backup("0/2000000", path)
    with open(os.path.join(path, backuper.PUBLISH_MARKER), "w") as f:
        json.dump({"backup_id": bid + 100, "start_lsn": "0/2000000"}, f)
    assert backuper.latest_usable() is None


def test_done_without_marker_falls_back_to_manifest(env):
    """旧版本遗留的 done 备份（无发布标记）仍按 manifest 校验后可用。"""
    legacy = os.path.join(env.backup_dir, "base-legacy")
    make_manifest_backup(legacy, "0/2000000")
    legacy_id = insert_backup("0/2000000", legacy)
    assert backuper.latest_usable()["id"] == legacy_id
    # 重启时会补写发布标记，但不会改变可用性
    backuper.reconcile()
    assert os.path.isfile(os.path.join(legacy, backuper.PUBLISH_MARKER))
    assert backuper.latest_usable()["id"] == legacy_id


def test_done_but_unpublished_row_allows_retry(env, monkeypatch):
    """done 行校验不通过（登记后目录损坏）→ 允许无需 force 重做。"""
    broken = os.path.join(env.backup_dir, "base-broken")
    os.makedirs(broken)
    insert_backup("0/9000000", broken)
    # 目录里没有 manifest → 不可用，且最新行是"坏的 done"
    assert backuper.latest_usable() is None

    def boom(_target):
        raise RuntimeError("boom")

    monkeypatch.setattr(backuper, "_pg_basebackup", boom)
    bid = backuper.start_backup(force=False)  # 不应抛 BusyError
    assert wait_finished(bid)["status"] == "failed"


def test_expire_before_ignores_none():
    """没有可用备份时不得过期任何核对点。"""
    assert checkpoints.expire_before(None) == []


# ------------------------------------------------------------------ 成功发布

def test_successful_publish_expires_old_checkpoints(env, monkeypatch):
    """成功发布后：新备份为可用，旧核对点按新起点过期。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    old_id = insert_backup("0/2000000", old)
    cp_old = insert_checkpoint("cp-old", "0/5000000")
    cp_new = insert_checkpoint("cp-new", "0/A000000")

    def ok_backup(target):
        make_manifest_backup(target, "0/9000000")

    monkeypatch.setattr(backuper, "_pg_basebackup", ok_backup)
    bid = backuper.start_backup(force=True)
    row = wait_finished(bid)

    assert row["status"] == "done"
    assert row["published_at"] is not None
    assert os.path.isdir(row["path"])
    assert os.path.basename(row["path"]).startswith("base-")
    assert os.path.isfile(os.path.join(row["path"], backuper.PUBLISH_MARKER))

    # 新旧备份目录并存：绝不移走/覆盖旧备份
    assert os.path.isdir(old)
    assert row["path"] != old
    assert state.one_row("SELECT status FROM backups WHERE id=?",
                         (old_id,))["status"] == "done"

    # 过期判定在 done 登记之后异步收尾，等它完成
    assert wait_status("checkpoints", "id", cp_old, "expired") == "expired"
    assert state.one_row("SELECT status FROM checkpoints WHERE id=?",
                         (cp_new,))["status"] == "ready"
    assert backuper.latest_usable()["id"] == bid


def test_older_checkpoint_not_expired_before_registration(env):
    """未登记的 running 行不参与过期判定。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    insert_backup("0/2000000", old)
    cp_old = insert_checkpoint("cp-old", "0/5000000")

    new = os.path.join(env.backup_dir, "base-new")
    make_manifest_backup(new, "0/9000000")
    insert_backup(None, new, status="running")
    assert backuper.latest_usable()["path"] == old
    checkpoints.expire_before(backuper.latest_usable())
    assert state.one_row("SELECT status FROM checkpoints WHERE id=?",
                         (cp_old,))["status"] == "ready"


def test_retry_after_failure_needs_no_force(env, monkeypatch):
    """失败后可直接再次发起备份（视为重试），成功后又需 force。"""
    old = os.path.join(env.backup_dir, "base-old")
    make_manifest_backup(old, "0/2000000")
    insert_backup("0/2000000", old)

    def boom(_target):
        raise RuntimeError("boom")

    monkeypatch.setattr(backuper, "_pg_basebackup", boom)
    first = backuper.start_backup(force=True)
    assert wait_finished(first)["status"] == "failed"

    def ok_backup(target):
        make_manifest_backup(target, "0/9000000")

    monkeypatch.setattr(backuper, "_pg_basebackup", ok_backup)
    second = backuper.start_backup(force=False)  # 不需要 force
    assert wait_finished(second)["status"] == "done"

    with pytest.raises(backuper.BusyError):
        backuper.start_backup(force=False)
