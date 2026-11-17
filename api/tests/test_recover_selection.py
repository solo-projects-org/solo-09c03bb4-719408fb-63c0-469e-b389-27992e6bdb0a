"""恢复任务选备份 / 过期判定的回归测试。

重点：恢复只能选"已完整发布的备份"，且失败或重启后旧核对点仍按原备份恢复。
"""
import os
import threading

import pytest

from app import backuper, checkpoints, config, recoverer, state


@pytest.fixture()
def env(tmp_path, monkeypatch):
    backup_dir = tmp_path / "backups"
    recovery_dir = tmp_path / "recoveries"
    for d in (backup_dir, recovery_dir, tmp_path / "state"):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(config, "RECOVERY_DIR", str(recovery_dir))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state" / "api.db"))
    monkeypatch.setattr(config, "ARCHIVE_DIR", str(tmp_path / "wal_archive"))
    monkeypatch.setattr(backuper, "_lock", threading.Lock())
    state.init_db()
    return tmp_path


def make_backup(path, start_lsn):
    from tests.test_backup_publish import make_manifest_backup
    make_manifest_backup(path, start_lsn)


def add_backup(path, start_lsn, status="done"):
    return state.execute(
        "INSERT INTO backups (path, status, start_lsn, timeline, created_at)"
        " VALUES (?,?,?,?,?)", (path, status, start_lsn, 1, state.now()))


def add_cp(name, lsn, status="ready"):
    return state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)",
        (name, lsn, "000000010000000000000001", status, state.now()))


def test_resolve_prefers_published_old_backup(env):
    """最新 done 不可用 / 新备份未登记时，旧核对点仍用原备份。"""
    old = str(env / "backups" / "base-old")
    make_backup(old, "0/2000000")
    old_id = add_backup(old, "0/2000000")
    add_cp("cp1", "0/5000000")

    # 新备份正在跑（未登记）
    running_path = str(env / "backups" / "base-new")
    make_backup(running_path, "0/9000000")
    add_backup(running_path, "0/9000000", status="running")

    cp = state.one_row("SELECT * FROM checkpoints WHERE name='cp1'")
    chosen = recoverer._resolve_backup(cp)
    assert chosen["id"] == old_id
    assert chosen["path"] == old


def test_resolve_rejects_expired_checkpoint(env):
    old = str(env / "backups" / "base-old")
    make_backup(old, "0/2000000")
    add_backup(old, "0/2000000")
    add_cp("cp1", "0/1000000", status="expired")
    cp = state.one_row("SELECT * FROM checkpoints WHERE name='cp1'")
    with pytest.raises(RuntimeError, match="过期"):
        recoverer._resolve_backup(cp)


def test_resolve_rejects_checkpoint_before_backup_and_expires_it(env):
    path = str(env / "backups" / "base-new")
    make_backup(path, "0/9000000")
    add_backup(path, "0/9000000")
    add_cp("cp-old", "0/5000000")
    cp = state.one_row("SELECT * FROM checkpoints WHERE name='cp-old'")
    with pytest.raises(RuntimeError, match="早于基准备份起点"):
        recoverer._resolve_backup(cp)
    assert state.one_row("SELECT status FROM checkpoints WHERE name='cp-old'")[
        "status"] == "expired"


def test_resolve_requires_a_published_backup(env):
    """没有任何可用备份（全部 failed/running）时不得启动恢复。"""
    path = str(env / "backups" / "base-half")
    make_backup(path, "0/9000000")
    add_backup(path, "0/9000000", status="failed")
    add_cp("cp1", "0/9500000")
    cp = state.one_row("SELECT * FROM checkpoints WHERE name='cp1'")
    with pytest.raises(RuntimeError, match="没有可用的基准备份"):
        recoverer._resolve_backup(cp)


def test_resume_after_restart_requeues_and_cleans_orphan(env):
    """重启：进行中任务重新排队并清理残留数据目录，ready 任务不动。"""
    from app import recoverer as r

    ddir = str(env / "recoveries" / "job-1")
    os.makedirs(ddir, exist_ok=True)
    jid = state.execute(
        "INSERT INTO jobs (checkpoint_name, status, data_dir, created_at,"
        " updated_at) VALUES (?,?,?,?,?)",
        ("cp1", "recovering", ddir, state.now(), state.now()))
    ready_dir = str(env / "recoveries" / "job-0")
    os.makedirs(ready_dir, exist_ok=True)
    rj = state.execute(
        "INSERT INTO jobs (checkpoint_name, status, data_dir, created_at,"
        " updated_at) VALUES (?,?,?,?,?)",
        ("cp1", "ready", ready_dir, state.now(), state.now()))

    # 隔离真实 pg_ctl / 实例探测
    calls = []
    orig_stop = r.stop_instance
    orig_alive = r._postgres_alive
    orig_ctl = r._pg_ctl
    orig_ctl_conn = r._control_conn
    r.stop_instance = lambda d: calls.append(("stop", d))
    r._postgres_alive = lambda d: False  # 实例已随重启消失
    r._pg_ctl = lambda d, a, log: calls.append(("pg_ctl", a))
    class DummyCon:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k):
            class Cur:
                def fetchone(self): return (1,)
            return Cur()
    r._control_conn = lambda: DummyCon()
    try:
        r.resume_after_restart()
    finally:
        r.stop_instance = orig_stop
        r._postgres_alive = orig_alive
        r._pg_ctl = orig_ctl
        r._control_conn = orig_ctl_conn

    row = state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))
    assert row["status"] == "queued"
    assert not os.path.isdir(ddir)          # 残留目录已清理
    assert state.one_row("SELECT status FROM jobs WHERE id=?",
                         (rj,))["status"] == "ready"
    assert ("stop", ddir) in calls
    assert ("pg_ctl", "start") in calls


def test_expire_only_uses_published_backup(env):
    path = str(env / "backups" / "base-old")
    make_backup(path, "0/2000000")
    add_backup(path, "0/2000000")
    add_cp("cp1", "0/2000000")   # 与备份起点相同（不早于）
    add_cp("cp2", "0/5000000")
    # 未发布的新备份起点更晚，但没有任何 done 行，不得过期任何核对点
    running_path = str(env / "backups" / "base-new")
    make_backup(running_path, "0/9000000")
    add_backup(running_path, "0/9000000", status="running")
    checkpoints.refresh(backuper.latest_usable())
    assert state.one_row("SELECT status FROM checkpoints WHERE name='cp1'")[
        "status"] == "ready"
    assert state.one_row("SELECT status FROM checkpoints WHERE name='cp2'")[
        "status"] == "ready"
