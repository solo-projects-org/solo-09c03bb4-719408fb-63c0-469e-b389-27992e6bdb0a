"""核对点状态维护：WAL 归档就绪、以及按已发布基准备份判定过期。

这里不导入 backuper，避免循环依赖；调用方把"当前已完整发布的基准备份"
传进来即可。核对点是否过期只取决于**已登记为 done 的备份起点**，运行中或
中断未登记的备份一律不参与判定。
"""
import os

from . import config, state
from .walutil import lsn_to_int


def refresh_waiting(archive_dir: str | None = None) -> None:
    """waiting_wal → ready：核对点所在的 WAL 段已经完整归档。"""
    archive_dir = archive_dir or config.ARCHIVE_DIR
    for cp in state.all_rows(
        "SELECT * FROM checkpoints WHERE status='waiting_wal'"
    ):
        if os.path.exists(os.path.join(archive_dir, cp["wal_file"])):
            state.execute(
                "UPDATE checkpoints SET status='ready', ready_at=? WHERE id=?"
                " AND status='waiting_wal'",
                (state.now(), cp["id"]),
            )


def expire_before(backup: dict | None) -> list[str]:
    """把恢复点早于给定基准备份起点的核对点标记为 expired。

    backup 必须是已经完整登记（status='done'）且内容有效的备份。
    返回被标记过期的核对点名，便于日志/测试断言。
    """
    if not backup or not backup.get("start_lsn"):
        return []
    start = lsn_to_int(backup["start_lsn"])
    expired: list[str] = []
    for cp in state.all_rows(
        "SELECT id, name, restore_lsn FROM checkpoints"
        " WHERE status IN ('waiting_wal','ready')"
    ):
        if lsn_to_int(cp["restore_lsn"]) < start:
            state.execute(
                "UPDATE checkpoints SET status='expired' WHERE id=?"
                " AND status IN ('waiting_wal','ready')",
                (cp["id"],),
            )
            expired.append(cp["name"])
    return expired


def refresh(backup: dict | None = None) -> list[str]:
    """刷新核对点状态：先处理 WAL 归档，再按备份起点判定过期。"""
    refresh_waiting()
    return expire_before(backup)
