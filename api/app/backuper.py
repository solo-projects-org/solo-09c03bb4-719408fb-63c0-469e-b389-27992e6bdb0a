"""物理基准备份：pg_basebackup 在线拷贝，源库全程可写。

发布协议（避免"旧备份被移走、新备份却没登记"的窗口）：

1. ``pg_basebackup`` 只写入私有 staging 目录 ``.staging-<id>-<uuid>``；
2. 读取并校验备份起始 LSN，在 staging 内写发布标记 ``.pitr-published``；
3. 原子 ``rename`` 为**唯一且不可变**的目录 ``base-<timestamp>-<uuid>``；
   绝不覆盖既有备份目录，恢复任务可以安全地并发拷贝旧备份；
4. 最后一步才在 SQLite 中把该行置为 ``done`` —— 这是唯一的发布提交点。
   在此之前崩溃/失败，新目录不会被任何恢复任务选中，旧 ``done`` 备份原样保留；
5. 成功登记后才按新备份起点把更早的核对点标记为 ``expired``。

启动时 :func:`reconcile` 会把仍为 ``running`` 的行收敛为 ``failed``、清理
未登记的 staging/半发布目录，并校验既有 ``done`` 备份目录内容与登记 LSN
是否一致，防止"显示完成却指向新数据"。
"""
import json
import os
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timezone

from . import checkpoints, config, state
from .walutil import lsn_to_int

PUBLISH_MARKER = ".pitr-published"
_OWNED_PREFIXES = (".staging-", ".tmp-", ".old-", "base-")


class BusyError(RuntimeError):
    """已有备份在进行 / 已存在可用备份且未指定 force。"""


_lock = threading.Lock()


# ---------------------------------------------------------------- 查询

def latest_done() -> dict | None:
    """最近一条已登记为 done 的备份（仅状态层面，供状态查询）。"""
    return state.one_row(
        "SELECT * FROM backups WHERE status='done' ORDER BY id DESC LIMIT 1"
    )


def latest_usable() -> dict | None:
    """最近一条**完整发布且内容匹配**的备份，恢复任务只能选它。

    - ``running`` / ``failed`` 行永远不参与选择；
    - 即使状态是 ``done``，目录缺失、发布标记或 backup_manifest 的起始 LSN
      与登记值不一致（例如目录被旧逻辑替换成了另一份备份）也不可用；
    - 最新的 done 不可用时继续向前找更早的 done，保证失败重做后旧核对点
      仍然可以用原备份恢复。
    """
    for row in state.all_rows(
        "SELECT * FROM backups WHERE status='done' ORDER BY id DESC"
    ):
        if is_published(row):
            return row
    return None


def is_published(backup: dict) -> bool:
    """校验一个 done 行确实指向它登记的那份完整备份。

    目录内的 ``.pitr-published`` 标记随 rename 一起成为备份的固有部分；
    标记中的 backup_id / start_lsn 与登记行一致即可快速判定（避免每次轮询
    都解析可能很大的 backup_manifest）。没有标记的旧版本备份则回退到解析
    manifest/label 比对起始 LSN。
    """
    if not backup or backup.get("status") != "done":
        return False
    path = backup.get("path")
    if not path or not os.path.isdir(path):
        return False
    if not (os.path.isfile(os.path.join(path, "backup_manifest"))
            or os.path.isfile(os.path.join(path, "backup_label"))):
        return False
    registered = backup.get("start_lsn")
    if not registered:
        return False
    try:
        registered_int = lsn_to_int(registered)
    except Exception:
        return False

    marker = _read_marker(path)
    if marker is not None:
        if marker.get("backup_id") != backup.get("id"):
            return False
        try:
            if lsn_to_int(str(marker.get("start_lsn"))) != registered_int:
                return False
        except Exception:
            return False
        return True

    # 兼容旧版本遗留、没有发布标记的 done 备份
    try:
        start_lsn, timeline = _read_start_lsn(path)
        if lsn_to_int(start_lsn) != registered_int:
            return False
        if (backup.get("timeline") is not None
                and int(backup["timeline"]) != timeline):
            return False
    except Exception:
        return False
    return True


# ---------------------------------------------------------------- 发起

def start_backup(force: bool = False) -> int:
    """启动一次基准备份（后台线程执行），返回 backups 表 id。

    上一次尝试失败/中断时，允许直接再次发起（视为重试，不需要 force）；
    只有在最近一次备份已经成功发布的情况下，才要求 force 才能重做。
    """
    with _lock:
        if state.one_row("SELECT id FROM backups WHERE status='running' LIMIT 1"):
            raise BusyError("已有基准备份任务正在进行")
        last = state.one_row("SELECT * FROM backups ORDER BY id DESC LIMIT 1")
        retry_after_failure = bool(
            last and (last["status"] == "failed"
                      or (last["status"] == "done" and not is_published(last)))
        )
        if not retry_after_failure and latest_usable() and not force:
            raise BusyError("已存在可用基准备份；如需重做请使用 force=true"
                            "（注意：早于新备份起点的核对点将过期）")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        final = os.path.join(
            config.BACKUP_DIR, f"base-{stamp}-{uuid.uuid4().hex[:8]}"
        )
        bid = state.execute(
            "INSERT INTO backups (path, status, created_at) VALUES (?,?,?)",
            (final, "running", state.now()),
        )
    threading.Thread(target=_run, args=(bid,), name=f"basebackup-{bid}",
                     daemon=True).start()
    return bid


# ---------------------------------------------------------------- 执行

def _run(bid: int) -> None:
    row = state.one_row("SELECT * FROM backups WHERE id=?", (bid,))
    if not row:
        return
    final = row["path"] or os.path.join(
        config.BACKUP_DIR, f"base-{bid}-{uuid.uuid4().hex[:8]}"
    )
    staging = os.path.join(
        config.BACKUP_DIR, f".staging-{bid}-{uuid.uuid4().hex[:8]}"
    )
    published = False
    try:
        os.makedirs(config.BACKUP_DIR, exist_ok=True)
        shutil.rmtree(staging, ignore_errors=True)
        _pg_basebackup(staging)
        start_lsn, timeline = _read_start_lsn(staging)
        _write_marker(staging, bid, start_lsn)
        _fsync_dir(staging)
        os.rename(staging, final)           # 原子发布到唯一目录，旧备份不受影响
        _fsync_dir(config.BACKUP_DIR)
        state.execute(
            "UPDATE backups SET status='done', path=?, start_lsn=?, timeline=?,"
            " size_bytes=?, finished_at=?, published_at=? WHERE id=?",
            (final, start_lsn, timeline, _dir_size(final), state.now(),
             state.now(), bid),
        )
        published = True
    except Exception as e:  # noqa: BLE001 - 任何失败都如实落库
        shutil.rmtree(staging, ignore_errors=True)
        # 只有确认未登记为 done，才删除刚发布出来的目录；
        # 绝不能因为发布后的收尾异常而删掉一份已经提交的备份。
        if not published:
            fresh = state.one_row("SELECT status FROM backups WHERE id=?", (bid,))
            if not fresh or fresh["status"] != "done":
                state.execute(
                    "UPDATE backups SET status='failed', error=?,"
                    " finished_at=? WHERE id=? AND status!='done'",
                    (str(e)[-2000:], state.now(), bid),
                )
                _safe_remove(final, _done_paths())
        return

    # 发布提交点之后：按新备份起点判定更早核对点过期。
    # 这里失败不能回滚已发布的备份，后台刷新线程也会兜底。
    try:
        done = state.one_row("SELECT * FROM backups WHERE id=?", (bid,))
        checkpoints.expire_before(done)
    except Exception:  # noqa: BLE001
        pass


def _pg_basebackup(target: str) -> None:
    env = dict(os.environ, PGPASSWORD=config.SOURCE_PASSWORD)
    subprocess.run(
        [
            "pg_basebackup",
            "-h", config.SOURCE_HOST,
            "-p", str(config.SOURCE_PORT),
            "-U", config.SOURCE_USER,
            "-D", target,
            "-Fp",           # plain 格式，目录可直接作为数据目录恢复
            "-X", "stream",  # 备份期间所需 WAL 一并流式拷贝，备份自洽
            "-c", "fast",
        ],
        check=True, env=env, capture_output=True, text=True, timeout=1800,
    )


# ---------------------------------------------------------------- 发布标记

def _write_marker(path: str, bid: int, start_lsn: str) -> None:
    marker = {
        "backup_id": bid,
        "start_lsn": start_lsn,
        "published_at": state.now(),
    }
    with open(os.path.join(path, PUBLISH_MARKER), "w") as f:
        json.dump(marker, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())


def _read_marker(path: str) -> dict | None:
    try:
        with open(os.path.join(path, PUBLISH_MARKER)) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return None
    except Exception:
        return {}


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# ---------------------------------------------------------------- 启动收敛

def reconcile() -> dict:
    """API 启动时修复备份发布状态（幂等）。

    - ``running`` 行视作上次进程中断，一律标 ``failed``（不自动"补登记"，
      避免把未完整走完发布流程的目录当成新备份）；
    - 清理中断行留下的 staging / 半发布目录；
    - 校验既有 ``done`` 行仍指向其登记的备份，失联或内容不匹配则如实标 failed；
      但**不删除**这类目录（保留现场，交由人工判断）；
    - 旧 ``done`` 目录从不删除，旧核对点因此仍可按原备份恢复。
    """
    report = {"interrupted": [], "invalid_done": [], "removed": []}
    interrupted_ids: list[int] = []

    for b in state.all_rows("SELECT * FROM backups WHERE status='running'"):
        state.execute(
            "UPDATE backups SET status='failed', error=?, finished_at=?"
            " WHERE id=? AND status='running'",
            ("备份进程中断（API 重启），该备份未完整发布", state.now(), b["id"]),
        )
        report["interrupted"].append(b["id"])
        interrupted_ids.append(b["id"])

    # 先校验 done 行，得到仍受保护的路径集合
    done_paths = _done_paths()
    for b in state.all_rows("SELECT * FROM backups WHERE status='done'"):
        try:
            _validate_done_row(b)
        except Exception as e:  # noqa: BLE001
            state.execute(
                "UPDATE backups SET status='failed', error=?, finished_at=?"
                " WHERE id=? AND status='done'",
                (f"启动校验失败：{e}"[-2000:], state.now(), b["id"]),
            )
            report["invalid_done"].append(b["id"])
    done_paths = _done_paths()

    # 只清理"中断的 running 行"拥有的未登记目录；done→failed 的现场保留。
    for bid in interrupted_ids:
        b = state.one_row("SELECT path FROM backups WHERE id=?", (bid,))
        if b and _safe_remove(b.get("path"), done_paths):
            report["removed"].append(b["path"])

    # 清理无任何数据库行引用的 staging / 半发布目录
    try:
        entries = list(os.scandir(config.BACKUP_DIR))
    except OSError:
        entries = []
    referenced = _all_paths()
    for entry in entries:
        if not entry.is_dir(follow_symlinks=False):
            continue
        name = entry.name
        if name.startswith((".staging-", ".tmp-", ".old-")):
            if entry.path not in done_paths:
                shutil.rmtree(entry.path, ignore_errors=True)
                report["removed"].append(entry.path)
        elif name.startswith("base-") and entry.path not in referenced:
            shutil.rmtree(entry.path, ignore_errors=True)
            report["removed"].append(entry.path)
    return report


def _validate_done_row(b: dict) -> None:
    path = b.get("path")
    if not path or not os.path.isdir(path):
        raise RuntimeError("备份目录不存在或不可访问")
    start_lsn, timeline = _read_start_lsn(path)
    if not b.get("start_lsn") or lsn_to_int(start_lsn) != lsn_to_int(b["start_lsn"]):
        raise RuntimeError(
            f"目录起始 LSN {start_lsn} 与登记值 {b.get('start_lsn')} 不一致")
    if b.get("timeline") is not None and int(b["timeline"]) != timeline:
        raise RuntimeError(
            f"目录时间线 {timeline} 与登记值 {b['timeline']} 不一致")
    if not (os.path.isfile(os.path.join(path, "backup_manifest"))
            or os.path.isfile(os.path.join(path, "backup_label"))):
        raise RuntimeError("备份目录缺少 backup_manifest/backup_label")
    if _read_marker(path) is None:
        # 兼容旧版本留下的 done 备份：校验通过后补写发布标记
        _write_marker(path, b["id"], start_lsn)


def _done_paths() -> set[str]:
    return {
        os.path.normpath(b["path"])
        for b in state.all_rows("SELECT path FROM backups WHERE status='done'")
        if b.get("path")
    }


def _all_paths() -> set[str]:
    return {
        os.path.normpath(b["path"])
        for b in state.all_rows("SELECT path FROM backups")
        if b.get("path")
    }


def _safe_remove(path: str | None, keep: set[str]) -> bool:
    """只删除 BACKUP_DIR 下由本服务命名的目录，且绝不碰 done 备份路径。"""
    if not path:
        return False
    norm = os.path.normpath(path)
    if norm in keep:
        return False
    base = os.path.basename(norm)
    if not base.startswith(_OWNED_PREFIXES):
        return False
    if os.path.dirname(norm) != os.path.normpath(config.BACKUP_DIR):
        return False
    if not os.path.isdir(norm):
        return False
    shutil.rmtree(norm, ignore_errors=True)
    return True


# ---------------------------------------------------------------- 备份元数据

def _read_start_lsn(path: str) -> tuple[str, int]:
    """从 backup_manifest 取备份起始 LSN 与时间线（失败则回退解析 backup_label）。"""
    try:
        with open(os.path.join(path, "backup_manifest")) as f:
            data = json.load(f)
        wr = data["WAL-Ranges"][0]
        return wr["Start-LSN"], int(wr["Timeline"])
    except Exception:
        with open(os.path.join(path, "backup_label")) as f:
            for line in f:
                if line.startswith("START WAL LOCATION"):
                    lsn = line.split(":", 1)[1].split("(")[0].strip()
                    return lsn, 1
        raise


def _dir_size(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total
