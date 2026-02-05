import os
import uuid
import queue
import asyncio
import subprocess
import threading
import datetime
import shutil
import signal
import time
from pathlib import Path
from typing import Optional

try:
    from config import (
        QUEUE_WORKER_CONCURRENCY,
        QUEUE_EVAL_INTERVAL_SEC,
        LOG_RETENTION_DAYS,
    )
except ImportError:
    QUEUE_WORKER_CONCURRENCY = 1
    QUEUE_EVAL_INTERVAL_SEC = 3
    LOG_RETENTION_DAYS = 7

from fastapi import FastAPI, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine, Column, String, DateTime, Date, Integer, Float, event, func
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# --- 基础配置 ---
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = BASE_DIR / "tasks_data"
DATA_ROOT.mkdir(exist_ok=True)
DB_PATH = f"sqlite:///{BASE_DIR}/database.db"

# --- 数据库定义 ---
Base = declarative_base()
engine = create_engine(
    DB_PATH,
    connect_args={"check_same_thread": False, "timeout": 20},
)

@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL") # 允许并发读写
    cursor.close()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True)
    name = Column(String)
    # 使用任务目录名，优先由任务名称派生，避免直接使用随机 id 作为文件夹名
    folder_name = Column(String, nullable=True)
    script_name = Column(String)
    conda_env = Column(String)
    cron_time = Column(String)
    upstream_id = Column(String, nullable=True)
    task_type = Column(String, default="factor")
    run_on_non_trading = Column(Integer, default=0)
    status = Column(String, default="Idle")
    last_run = Column(String, nullable=True)

class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True)
    task_id = Column(String, index=True)
    trigger_type = Column(String)
    status = Column(String, default="Running")
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    exit_code = Column(Integer, nullable=True)
    log_path = Column(String)
    attempt = Column(Integer, default=1)
    duration_ms = Column(Integer, nullable=True)
    run_date = Column(Date, nullable=True)
    output_path = Column(String, nullable=True)
    message = Column(String, nullable=True)

class TaskRun(Base):
    __tablename__ = "task_runs"
    run_id = Column(String, primary_key=True)
    task_id = Column(String, index=True)
    status = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    exit_code = Column(Integer, nullable=True)
    duration = Column(Float, nullable=True)
    log_path = Column(String)


class JobQueue(Base):
    __tablename__ = "job_queue"
    id = Column(String, primary_key=True)
    task_id = Column(String, index=True)
    trigger_type = Column(String)
    run_date = Column(Date)
    status = Column(String)  # Pending / Wait / Runnable / Running / Success / Failed / Skipped / Stopped
    run_id = Column(String, nullable=True)
    message = Column(String, nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


Base.metadata.create_all(bind=engine)


def _ensure_tasks_columns():
    """为已有数据库补充 tasks 表新增列（folder_name 等），避免 no such column 错误。"""
    from sqlalchemy import text

    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(tasks)"))
        cols = {row[1] for row in r}
        if "folder_name" not in cols:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN folder_name VARCHAR"))
        if "task_type" not in cols:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN task_type VARCHAR"))
        if "run_on_non_trading" not in cols:
            conn.execute(text("ALTER TABLE tasks ADD COLUMN run_on_non_trading INTEGER"))
        conn.commit()


def _ensure_runs_columns():
    """为已有数据库补充 runs 表新增列（run_date、output_path、message 等），避免 no such column 错误。"""
    from sqlalchemy import text
    # 模型里可能后加的、可空列：(列名, SQL 类型)
    optional_columns = [
        ("run_date", "DATE"),
        ("output_path", "VARCHAR"),
        ("message", "VARCHAR"),
    ]
    with engine.connect() as conn:
        r = conn.execute(text("PRAGMA table_info(runs)"))
        cols = {row[1] for row in r}
        for name, sql_type in optional_columns:
            if name not in cols:
                conn.execute(text(f"ALTER TABLE runs ADD COLUMN {name} {sql_type}"))
        conn.commit()


_ensure_tasks_columns()
_ensure_runs_columns()


def _sanitize_task_folder_name(name: str) -> str:
    """根据任务名称生成安全的文件夹名。"""
    name = (name or "").strip()
    if not name:
        return "task"
    safe_chars = []
    for ch in name:
        if ch.isalnum() or ch in "-_ ":
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    safe = "".join(safe_chars)
    # 把连续空格压缩成单个下划线
    safe = "_".join(filter(None, safe.split()))
    return safe or "task"


def _ensure_unique_task_folder_name(base_name: str) -> str:
    """确保在 tasks_data 下文件夹名唯一，如已存在则追加序号。"""
    existing = {p.name for p in DATA_ROOT.iterdir() if p.is_dir()}
    candidate = base_name
    suffix = 1
    while candidate in existing:
        suffix += 1
        candidate = f"{base_name}_{suffix}"
    return candidate


def _get_task_dir(task: Task) -> Path:
    """根据 Task 记录获取对应的任务目录，优先使用 folder_name。"""
    folder_name = getattr(task, "folder_name", None)
    if folder_name:
        return DATA_ROOT / folder_name
    # 兼容老数据：老任务仍然使用 id 作为文件夹名
    return DATA_ROOT / task.id


def _is_trading_day(day: datetime.date) -> bool:
    """简易交易日判断：周一至周五为交易日。"""
    return day.weekday() < 5


def _parse_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _cleanup_task_logs(task_dir: Path):
    """清理超过保留天数的日志文件。"""
    retention_days = max(int(LOG_RETENTION_DAYS or 0), 0)
    if retention_days <= 0:
        return
    log_dir = task_dir / "logs"
    if not log_dir.exists():
        return
    cutoff = time.time() - retention_days * 86400
    for log_file in log_dir.glob("*.log"):
        try:
            if log_file.stat().st_mtime < cutoff:
                log_file.unlink(missing_ok=True)
        except Exception:
            pass


def _cleanup_task_outputs(task: Task, task_dir: Path, current_output: Optional[Path]):
    """输出目录由任务脚本自行管理，系统不做自动清理。"""
    pass


def _resolve_run_log_file(task_dir: Path, log_path: Optional[str]) -> Path:
    if not log_path:
        return task_dir / "logs" / "unknown.log"
    log_file = Path(log_path)
    if not log_file.is_absolute():
        log_file = task_dir / log_file
    return log_file


def _stop_process_windows(process: subprocess.Popen) -> tuple[bool, str, Optional[int]]:
    if process.poll() is not None:
        return True, "process already exited", process.returncode
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.terminate()
        try:
            process.wait(timeout=2)
            return True, "stopped", process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
            return True, "force killed", process.returncode
    except Exception as exc:
        try:
            process.kill()
            process.wait(timeout=2)
            return True, f"kill due to error: {exc}", process.returncode
        except Exception as kill_exc:
            return False, f"failed to stop process: {kill_exc}", process.returncode


def _mark_stopped(
    db: Session,
    task_id: str,
    run_id: Optional[str] = None,
    job_id: Optional[str] = None,
    message: Optional[str] = None,
    exit_code: Optional[int] = None,
    update_running_jobs: bool = False,
):
    now = datetime.datetime.now()
    task = db.query(Task).filter(Task.id == task_id).first()
    if task:
        task.status = "Stopped"
        if run_id:
            task.last_run = run_id
    run = None
    if run_id:
        run = db.query(Run).filter(Run.id == run_id, Run.task_id == task_id).first()
    if not run:
        run = (
            db.query(Run)
            .filter(Run.task_id == task_id)
            .order_by(Run.start_time.desc())
            .first()
        )
    if run:
        run.status = "Stopped"
        run.end_time = run.end_time or now
        if run.start_time and run.end_time:
            run.duration_ms = int((run.end_time - run.start_time).total_seconds() * 1000)
        if exit_code is not None:
            run.exit_code = exit_code
        if message:
            run.message = message
    if job_id:
        job = db.query(JobQueue).filter(JobQueue.id == job_id).first()
        if job:
            job.status = "Stopped"
            job.updated_at = now
            if message:
                job.message = message
    if update_running_jobs:
        (
            db.query(JobQueue)
            .filter(JobQueue.task_id == task_id, JobQueue.status == "Running")
            .update({"status": "Stopped", "updated_at": now}, synchronize_session=False)
        )
    db.commit()

def enqueue_job(task_id: str, trigger_type: str = "cron"):
    """将任务入队：插入 job_queue 一条 status=Pending、run_date=today 的记录。

    严格限制：同一 task_id 在队列中（Pending/Wait/Runnable）或运行中（Running）只能存在一条记录。
    若已存在未完成实例，则本次入队直接忽略。

    返回值：
        True  - 成功入队
        False - 已有实例在队列或运行中，本次未入队
    """
    db = SessionLocal()
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        if not task:
            return False
        # 若该任务已有未完成实例在队列或运行中，则不再重复入队
        existing = (
            db.query(JobQueue)
            .filter(
                JobQueue.task_id == task_id,
                JobQueue.status.in_(["Pending", "Wait", "Runnable", "Running"]),
            )
            .first()
        )
        if existing:
            return False
        now = datetime.datetime.now()
        run_date = datetime.date.today()
        if trigger_type == "cron" and not _is_trading_day(run_date) and not bool(task.run_on_non_trading or 0):
            return False
        job_id = str(uuid.uuid4())
        job = JobQueue(
            id=job_id,
            task_id=task_id,
            trigger_type=trigger_type,
            run_date=run_date,
            status="Pending",
            created_at=now,
            updated_at=now,
        )
        db.add(job)
        db.commit()
        _event_queue.put_nowait("update")
        return True
    finally:
        db.close()


def _upstream_success_for_date(db: Session, upstream_id: str, run_date: datetime.date) -> bool:
    """当天该上游最后一次 Run 是否为 Success（按 start_time 降序取第一条）。"""
    r = (
        db.query(Run)
        .filter(Run.task_id == upstream_id, Run.run_date == run_date)
        .order_by(Run.start_time.desc())
        .limit(1)
        .first()
    )
    return r is not None and r.status == "Success"


def _upstream_job_running(db: Session, upstream_id: str) -> bool:
    """JobQueue 中是否存在上游任务任意 run_date 的 Running job。"""
    return (
        db.query(JobQueue)
        .filter(JobQueue.task_id == upstream_id, JobQueue.status == "Running")
        .first()
        is not None
    )


def _downstream_job_running(db: Session, task_id: str) -> bool:
    """是否存在以 task_id 为上游的下游任务且其任意 run_date 的 job 正在 Running。"""
    downstream_ids = [
        r[0] for r in db.query(Task.id).filter(Task.upstream_id == task_id).all()
    ]
    if not downstream_ids:
        return False
    return (
        db.query(JobQueue)
        .filter(JobQueue.task_id.in_(downstream_ids), JobQueue.status == "Running")
        .first()
        is not None
    )


def evaluate_queue_item(db: Session, job: JobQueue) -> str:
    """
    评估单条队列表项：若依赖满足且无上游/下游在跑则返回 'Runnable'，否则返回 'Wait'。
    不写库，仅返回目标状态。
    """
    task = db.query(Task).filter(Task.id == job.task_id).first()
    if not task:
        return "Wait"
    if task.upstream_id:
        if _upstream_job_running(db, task.upstream_id):
            return "Wait"
        if not _upstream_success_for_date(db, task.upstream_id, job.run_date):
            return "Wait"
    if _downstream_job_running(db, job.task_id):
        return "Wait"
    return "Runnable"


def evaluate_pending_and_wait(db: Session):
    """扫描 status in (Pending, Wait) 的队列表项，按依赖更新为 Runnable 或 Wait。"""
    now = datetime.datetime.now()
    for job in db.query(JobQueue).filter(JobQueue.status.in_(["Pending", "Wait"])).all():
        new_status = evaluate_queue_item(db, job)
        if job.status != new_status:
            job.status = new_status
            job.updated_at = now
    db.commit()


_consumer_stop = threading.Event()


def _eval_loop():
    """后台线程：每 QUEUE_EVAL_INTERVAL_SEC 秒执行一次依赖评估。"""
    while not _consumer_stop.is_set():
        _consumer_stop.wait(timeout=QUEUE_EVAL_INTERVAL_SEC)
        if _consumer_stop.is_set():
            break
        db = SessionLocal()
        try:
            evaluate_pending_and_wait(db)
        except Exception:
            pass
        finally:
            db.close()


# --- 事件推送逻辑 ---
_event_queue = queue.Queue()
_sse_queues: set = set()
_client_sse_queues: dict[str, asyncio.Queue] = {}
_task_processes: dict[str, subprocess.Popen] = {}
_task_stop_requests: set[str] = set()
_task_process_lock = threading.Lock()

async def _sse_broadcast_loop():
    # 事件内容仅作触发，不区分类型；取到即向所有 SSE 连接广播 refresh
    while True:
        await asyncio.sleep(0.2)
        try:
            while True:
                _event_queue.get_nowait()
                msg = "data: refresh\n\n"
                for q in list(_sse_queues):
                    try:
                        q.put_nowait(msg)
                    except Exception:
                        pass
        except queue.Empty:
            pass

# --- 消费者：认领 Runnable、创建 Run、执行脚本、更新队列 ---
def _run_task_script(db: Session, task_id: str, run_id: str, trigger_type: str, run_date: datetime.date) -> str:
    """
    执行已存在的 Run 对应脚本。假定 Run 已创建且上游已满足。
    若执行前再次检查发现上游未完成，返回 "Wait"；否则执行并返回终态 "Success"|"Failed"|"Stopped"。
    """
    run = db.query(Run).filter(Run.id == run_id, Run.task_id == task_id).first()
    task = db.query(Task).filter(Task.id == task_id).first()
    if not run or not task:
        return "Failed"
    task_dir = _get_task_dir(task)
    script_dir = task_dir / "script"
    script_path = task_dir / "script" / task.script_name
    task_output_dir = task_dir / "output"
    log_file = _resolve_run_log_file(task_dir, run.log_path)

    upstream_task_dir = ""
    upstream_script_dir = ""
    if task.upstream_id:
        upstream_task = db.query(Task).filter(Task.id == task.upstream_id).first()
        if not upstream_task:
            return "Wait"
        if _upstream_job_running(db, task.upstream_id):
            return "Wait"
        if not _upstream_success_for_date(db, task.upstream_id, run_date):
            return "Wait"
        upstream_task_dir = str(_get_task_dir(upstream_task))
        upstream_script_dir = str(upstream_task_dir / "script")
    env_vars = os.environ.copy()
    env_vars.update({
        "SCRIPT_DIR" : str(script_dir),
        "SCRIPT_PATH": str(script_path),
        "TASK_ID": task_id,
        "OUTPUT_PATH": str(task_output_dir),
        "OUTPUT_DIR": str(task_output_dir),
        "PYTHONUNBUFFERED": "1",
        "PYTHONIOENCODING": "utf-8",
    })
    if task.upstream_id:
        env_vars["UPSTREAM_TASK_DIR"] = upstream_task_dir
        env_vars["UPSTREAM_SCRIPT_DIR"] = upstream_script_dir
    start_time = run.start_time
    exit_code = None
    status = "Failed"
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*20} START {datetime.datetime.now()} {'='*20}\n")
            f.flush()
            is_windows = os.name == "nt"
            creationflags = 0
            if is_windows and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            process = subprocess.Popen(
                f'conda run --no-capture-output -n {task.conda_env} python -u "{script_path}"',
                shell=True,
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env_vars,
                creationflags=creationflags,
                start_new_session=not is_windows,
            )
            with _task_process_lock:
                _task_processes[task_id] = process
            process.wait()
            with _task_process_lock:
                was_stopped = task_id in _task_stop_requests
                _task_stop_requests.discard(task_id)
            if was_stopped:
                status = "Stopped"
            else:
                status = "Success" if process.returncode == 0 else "Failed"
            exit_code = process.returncode
    except Exception as exc:
        status = "Failed"
        run.message = f"Execution error: {exc}"
    finally:
        with _task_process_lock:
            _task_processes.pop(task_id, None)
    end_time = datetime.datetime.now()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    run.end_time = end_time
    run.exit_code = exit_code
    run.status = status
    run.duration_ms = duration_ms
    task.status = status
    task.last_run = run_id
    db.commit()
    _cleanup_task_logs(task_dir)
    _cleanup_task_outputs(task, task_dir, None)
    _event_queue.put_nowait("update")
    return status


def _can_claim_job(db: Session, job: JobQueue) -> bool:
    """
    认领原子时刻三重校验：依赖就绪、无上游在跑、无下游在跑。
    全部通过返回 True，任一项不满足返回 False。
    """
    task = db.query(Task).filter(Task.id == job.task_id).first()
    if not task:
        return False
    if task.upstream_id:
        if not _upstream_success_for_date(db, task.upstream_id, job.run_date):
            return False
        if _upstream_job_running(db, task.upstream_id):
            return False
    if _downstream_job_running(db, job.task_id):
        return False
    return True


def _claim_runnable_job(db: Session):
    """
    认领一条 Runnable 且同一 task_id 无 Running 的队列表项。
    仅当三重校验（依赖就绪、无上游在跑、无下游在跑）全部通过后才将 job 置为 Running。
    返回 (JobQueue, True) 若认领成功并已置为 Running；返回 (None, False) 若无可认领。
    """
    running_task_ids = {
        r[0] for r in db.query(JobQueue.task_id).filter(JobQueue.status == "Running").distinct().all()
    }
    candidates = (
        db.query(JobQueue)
        .filter(JobQueue.status == "Runnable")
        .order_by(JobQueue.created_at)
        .all()
    )
    now = datetime.datetime.now()
    for candidate in candidates:
        if candidate.task_id in running_task_ids:
            continue
        if not _can_claim_job(db, candidate):
            candidate.status = "Wait"
            candidate.updated_at = now
            continue
        rows = (
            db.query(JobQueue)
            .filter(JobQueue.id == candidate.id, JobQueue.status == "Runnable")
            .update({"status": "Running", "updated_at": now}, synchronize_session=False)
        )
        db.commit()
        if rows == 0:
            continue
        db.refresh(candidate)
        return candidate, True
    db.commit()
    return None, False


def _consumer_worker():
    """单个消费者线程：循环认领 Runnable、创建 Run、执行、更新队列。"""
    while not _consumer_stop.is_set():
        db = SessionLocal()
        job = None
        try:
            job, claimed = _claim_runnable_job(db)
            if not claimed or not job:
                db.close()
                _consumer_stop.wait(timeout=1.0)
                continue
            task_id = job.task_id
            trigger_type = job.trigger_type
            run_date = job.run_date
            task = db.query(Task).filter(Task.id == task_id).first()
            if not task:
                job.status = "Failed"
                job.message = "Task not found"
                job.updated_at = datetime.datetime.now()
                db.commit()
                db.close()
                continue
            start_time = datetime.datetime.now()
            run_id = str(uuid.uuid4())
            task_dir = _get_task_dir(task)
            log_dir = task_dir / "logs"
            log_dir.mkdir(exist_ok=True)
            log_filename = f"{start_time.strftime('%Y%m%d_%H%M%S')}_{run_id}.log"
            log_path = str(Path("logs") / log_filename)
            last_attempt = db.query(func.max(Run.attempt)).filter(Run.task_id == task_id).scalar() or 0
            run = Run(
                id=run_id,
                task_id=task_id,
                trigger_type=trigger_type,
                status="Running",
                start_time=start_time,
                run_date=run_date,
                log_path=log_path,
                attempt=last_attempt + 1,
                output_path=None,
            )
            db.add(run)
            task.status = "Running"
            task.last_run = run_id
            job.run_id = run_id
            db.commit()
            final_status = _run_task_script(db, task_id, run_id, trigger_type, run_date)
            if final_status == "Wait":
                job.status = "Wait"
                run = db.query(Run).filter(Run.id == run_id).first()
                if run:
                    run.status = "Failed"
                    run.end_time = datetime.datetime.now()
                    run.message = "Upstream not ready (race)"
                db.commit()
            else:
                job.status = final_status
                job.updated_at = datetime.datetime.now()
                db.commit()
        except Exception as e:
            if job:
                job.status = "Failed"
                job.message = str(e)
                job.updated_at = datetime.datetime.now()
                try:
                    db.commit()
                except Exception:
                    pass
            try:
                db.close()
            except Exception:
                pass
        else:
            db.close()
        _event_queue.put_nowait("update")
        if not _consumer_stop.is_set():
            time.sleep(0.1)

# --- 调度器初始化（清空旧 job 表，不再兼容 execute_factor_task）---
from sqlalchemy import text
try:
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM apscheduler_jobs"))
        conn.commit()
except Exception:
    pass  # 表不存在或首次运行则忽略
scheduler = BackgroundScheduler(jobstores={'default': SQLAlchemyJobStore(engine=engine)})
scheduler.start()

# --- FastAPI 接口 ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(BASE_DIR / "static" / "favicon.ico")

_eval_thread: Optional[threading.Thread] = None
_consumer_threads: list = []


@app.on_event("startup")
async def startup():
    # 将上次崩溃时残留的 Running 任务置为 Unknown，由使用者看日志排查
    db = SessionLocal()
    try:
        db.query(Task).filter(Task.status == "Running").update({"status": "Unknown"}, synchronize_session=False)
        db.query(Run).filter(Run.status == "Running").update({"status": "Unknown"}, synchronize_session=False)
        db.query(JobQueue).filter(JobQueue.status == "Running").update({"status": "Runnable"}, synchronize_session=False)
        db.commit()
    finally:
        db.close()
    global _eval_thread, _consumer_threads
    _eval_thread = threading.Thread(target=_eval_loop, daemon=True)
    _eval_thread.start()
    for _ in range(QUEUE_WORKER_CONCURRENCY):
        t = threading.Thread(target=_consumer_worker, daemon=True)
        t.start()
        _consumer_threads.append(t)
    asyncio.create_task(_sse_broadcast_loop())

@app.on_event("shutdown")
def on_shutdown():
    _consumer_stop.set()
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        engine.dispose()
    except Exception:
        pass

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}

@app.get("/api/events")
async def sse_events(request: Request):
    client_id = request.client.host if request.client else "unknown"
    if client_id in _client_sse_queues:
        old_q = _client_sse_queues[client_id]
        try:
            old_q.put_nowait(None)
        except Exception:
            pass
        _sse_queues.discard(old_q)
        del _client_sse_queues[client_id]
    q = asyncio.Queue()
    _sse_queues.add(q)
    _client_sse_queues[client_id] = q

    async def stream():
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    msg = "data: ping\n\n"
                if msg is None:
                    break
                try:
                    yield msg
                except (BrokenPipeError, ConnectionResetError, asyncio.CancelledError, ConnectionError):
                    break
        finally:
            _sse_queues.discard(q)
            if _client_sse_queues.get(client_id) == q:
                _client_sse_queues.pop(client_id, None)
    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )

@app.get("/api/envs")
def get_envs():
    try:
        output = subprocess.check_output("conda env list", shell=True, text=True)
        return [line.split()[0] for line in output.splitlines() if line and not line.startswith("#")]
    except Exception:
        return ["base"]

@app.get("/api/tasks")
def list_tasks(db: Session = Depends(get_db)):
    tasks = db.query(Task).all()
    res = []
    for t in tasks:
        job = scheduler.get_job(t.id)
        latest_run = (
            db.query(Run)
            .filter(Run.task_id == t.id)
            .order_by(Run.start_time.desc())
            .first()
        )
        last_run_time = latest_run.start_time.strftime("%Y-%m-%d %H:%M:%S") if latest_run and latest_run.start_time else "-"
        last_run_ts = latest_run.start_time.isoformat() if latest_run and latest_run.start_time else None
        last_status = latest_run.status if latest_run else "Idle"
        res.append({
            "id": t.id, "name": t.name, "script": t.script_name,
            "env": t.conda_env, "schedule": t.cron_time, "status": last_status,
            "last_run": last_run_time,
            "last_run_ts": last_run_ts,
            "next_run": job.next_run_time.strftime("%H:%M:%S") if job and job.next_run_time else "Paused",
            "task_type": t.task_type or "factor",
            "run_on_non_trading": bool(t.run_on_non_trading or 0),
        })
    return res


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    job = scheduler.get_job(task.id)
    latest_run = (
        db.query(Run)
        .filter(Run.task_id == task.id)
        .order_by(Run.start_time.desc())
        .first()
    )
    last_run_time = latest_run.start_time.strftime("%Y-%m-%d %H:%M:%S") if latest_run and latest_run.start_time else "-"
    last_run_ts = latest_run.start_time.isoformat() if latest_run and latest_run.start_time else None
    last_status = latest_run.status if latest_run else "Idle"
    return {
        "id": task.id,
        "name": task.name,
        "script": task.script_name,
        "env": task.conda_env,
        "time": task.cron_time,
        "schedule": task.cron_time,
        "status": last_status,
        "last_run": last_run_time,
        "last_run_ts": last_run_ts,
        "next_run": job.next_run_time.strftime("%H:%M:%S") if job and job.next_run_time else "Paused",
        "task_type": task.task_type or "factor",
        "run_on_non_trading": bool(task.run_on_non_trading or 0),
        "upstream_id": task.upstream_id,
    }

def _format_run(run: TaskRun):
    return {
        "run_id": run.run_id,
        "status": run.status,
        "start_time": run.start_time.strftime("%Y-%m-%d %H:%M:%S") if run.start_time else None,
        "end_time": run.end_time.strftime("%Y-%m-%d %H:%M:%S") if run.end_time else None,
        "exit_code": run.exit_code,
        "duration": run.duration,
        "log_path": run.log_path,
    }

def _format_run_from_run(run: Run):
    return {
        "run_id": run.id,
        "id": run.id,
        "status": run.status,
        "start_time": run.start_time.strftime("%Y-%m-%d %H:%M:%S") if run.start_time else None,
        "started_at": run.start_time.strftime("%Y-%m-%d %H:%M:%S") if run.start_time else None,
        "end_time": run.end_time.strftime("%Y-%m-%d %H:%M:%S") if run.end_time else None,
        "exit_code": run.exit_code,
        "duration": run.duration_ms / 1000.0 if run.duration_ms is not None else None,
        "log_path": run.log_path,
    }

@app.get("/api/tasks/{task_id}/runs")
def list_task_runs(task_id: str, limit: int = 20, db: Session = Depends(get_db)):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    runs = (
        db.query(Run)
        .filter(Run.task_id == task_id)
        .order_by(Run.start_time.desc())
        .limit(min(limit, 200))
        .all()
    )
    return [_format_run_from_run(run) for run in runs]

@app.get("/api/runs")
def list_runs_global(limit: int = 50, db: Session = Depends(get_db)):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    runs = (
        db.query(Run)
        .order_by(Run.start_time.desc())
        .limit(min(limit, 500))
        .all()
    )
    return [_format_run_from_run(run) for run in runs]

@app.post("/api/tasks")
async def create_task(
    name: str = Form(...), conda_env: str = Form(...), time: str = Form(...),
    upstream_id: Optional[str] = Form(None),
    task_type: str = Form("factor"),
    run_on_non_trading: Optional[str] = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    task_id = str(uuid.uuid4())
    base_folder = _sanitize_task_folder_name(name)
    folder_name = _ensure_unique_task_folder_name(base_folder)
    task_dir = DATA_ROOT / folder_name
    for sub in ["script", "logs", "output"]:
        (task_dir / sub).mkdir(parents=True, exist_ok=True)

    with open(task_dir / "script" / file.filename, "wb") as f:
        f.write(await file.read())

    new_task = Task(
        id=task_id, name=name, folder_name=folder_name,
        script_name=file.filename, conda_env=conda_env, cron_time=time, upstream_id=upstream_id,
        task_type=task_type or "factor",
        run_on_non_trading=1 if _parse_bool(run_on_non_trading) else 0,
    )
    db.add(new_task); db.commit()
    
    h, m = time.split(":")
    scheduler.add_job(enqueue_job, 'cron', hour=int(h), minute=int(m), id=task_id, args=[task_id, "cron"])
    _event_queue.put_nowait("changed")
    return {"id": task_id}


@app.put("/api/tasks/{task_id}")
async def update_task(
    task_id: str,
    name: str = Form(...),
    conda_env: str = Form(...),
    time: str = Form(...),
    upstream_id: Optional[str] = Form(None),
    task_type: str = Form("factor"),
    run_on_non_trading: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.name = name
    task.conda_env = conda_env
    task.cron_time = time
    task.upstream_id = upstream_id
    task.task_type = task_type or "factor"
    task.run_on_non_trading = 1 if _parse_bool(run_on_non_trading) else 0

    db.commit()

    # 同步更新调度配置
    try:
        h, m = time.split(":")
        job = scheduler.get_job(task_id)
        if job:
            scheduler.reschedule_job(task_id, trigger='cron', hour=int(h), minute=int(m))
        else:
            scheduler.add_job(enqueue_job, 'cron', hour=int(h), minute=int(m), id=task_id, args=[task_id, "cron"])
    except Exception as exc:
        # 仅记录错误，不阻塞任务基础信息更新
        raise HTTPException(status_code=500, detail=f"Update schedule failed: {exc}")

    _event_queue.put_nowait("changed")
    return {"status": "success"}

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str, bg: BackgroundTasks, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task_dir = _get_task_dir(task)
    # 1. 立即从数据库和调度器中移除
    db.delete(task)
    db.commit()
    if scheduler.get_job(task_id):
        scheduler.remove_job(task_id)

    # 2. 物理删除交给后台，不阻塞 API 响应，防止死锁
    bg.add_task(shutil.rmtree, task_dir, ignore_errors=True)
    
    _event_queue.put_nowait("changed")
    return {"status": "success"}

@app.get("/api/queue")
def list_queue(limit: int = 100, db: Session = Depends(get_db)):
    """返回 status 为 Pending/Wait/Runnable（及 Running）的队列表项，便于排查。"""
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    jobs = (
        db.query(JobQueue)
        .filter(JobQueue.status.in_(["Pending", "Wait", "Runnable", "Running"]))
        .order_by(JobQueue.created_at.desc())
        .limit(min(limit, 500))
        .all()
    )
    task_ids = {j.task_id for j in jobs}
    tasks = {t.id: t for t in db.query(Task).filter(Task.id.in_(task_ids)).all()} if task_ids else {}
    return [
        {
            "id": j.id,
            "task_id": j.task_id,
            "task_name": tasks.get(j.task_id).name if tasks.get(j.task_id) else "-",
            "trigger_type": j.trigger_type,
            "run_date": j.run_date.isoformat() if j.run_date else None,
            "status": j.status,
            "created_at": j.created_at.strftime("%Y-%m-%d %H:%M:%S") if j.created_at else None,
            "updated_at": j.updated_at.strftime("%Y-%m-%d %H:%M:%S") if j.updated_at else None,
        }
        for j in jobs
    ]


@app.post("/api/queue/{job_id}/cancel")
def cancel_queue_job(job_id: str, db: Session = Depends(get_db)):
    """未执行则从队列移除，执行中则杀掉进程。"""
    job = db.query(JobQueue).filter(JobQueue.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Queue job not found")
    if job.status in ("Pending", "Wait", "Runnable"):
        db.delete(job)
        db.commit()
        _event_queue.put_nowait("update")
        return {"status": "cancelled"}
    if job.status == "Running":
        task_id = job.task_id
        with _task_process_lock:
            process = _task_processes.get(task_id)
            if process and process.poll() is None:
                _task_stop_requests.add(task_id)
            else:
                process = None
        if process:
            stopped, message, exit_code = _stop_process_windows(process)
            _mark_stopped(
                db,
                task_id=task_id,
                run_id=job.run_id,
                job_id=job.id,
                message=message,
                exit_code=exit_code,
            )
            if not stopped:
                raise HTTPException(status_code=500, detail=message)
        else:
            _mark_stopped(
                db,
                task_id=task_id,
                run_id=job.run_id,
                job_id=job.id,
                message="process not found",
                exit_code=None,
            )
        _event_queue.put_nowait("update")
        return {"status": "stopped"}
    raise HTTPException(status_code=400, detail="Job already finished")


@app.post("/api/run_now/{task_id}")
def run_now(task_id: str, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    queued = enqueue_job(task_id, "manual")
    if not queued:
        # 队列或运行中已存在该任务实例
        return {"status": "skipped", "reason": "task already queued or running"}
    return {"status": "queued"}

@app.post("/api/stop/{task_id}")
def stop_task(task_id: str, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    with _task_process_lock:
        process = _task_processes.get(task_id)
        if process and process.poll() is None:
            _task_stop_requests.add(task_id)
        else:
            process = None
    if not process:
        raise HTTPException(status_code=409, detail="Task is not running")
    stopped, message, exit_code = _stop_process_windows(process)
    _mark_stopped(
        db,
        task_id=task_id,
        run_id=task.last_run,
        job_id=None,
        message=message,
        exit_code=exit_code,
        update_running_jobs=True,
    )
    if not stopped:
        raise HTTPException(status_code=500, detail=message)
    _event_queue.put_nowait("changed")
    return {"status": "stopped"}

@app.get("/api/logs/{task_id}")
def get_log(task_id: str, run_id: Optional[str] = None, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task_dir = _get_task_dir(task)
    log_dir = task_dir / "logs"
    if not log_dir.exists():
        return {"content": "暂无日志数据。", "run_id": None}
    if run_id:
        run = db.query(Run).filter(Run.id == run_id, Run.task_id == task_id).first()
        if not run:
            return {"content": "未找到对应运行记录。", "run_id": None}
        log_file = _resolve_run_log_file(task_dir, run.log_path)
        if log_file.exists():
            return {"content": log_file.read_text(encoding="utf-8", errors="replace"), "run_id": run.id}
        return {"content": "日志文件不存在。", "run_id": run.id}

    run = db.query(Run).filter(Run.task_id == task_id).order_by(Run.start_time.desc()).first()
    if run:
        log_file = _resolve_run_log_file(task_dir, run.log_path)
        if log_file.exists():
            return {"content": log_file.read_text(encoding="utf-8", errors="replace"), "run_id": run.id}
        return {"content": "日志文件不存在。", "run_id": run.id}

    log_files = list(log_dir.glob("*.log"))
    if not log_files:
        return {"content": "暂无日志数据。", "run_id": None}
    latest = max(log_files, key=lambda p: p.stat().st_mtime)
    return {"content": latest.read_text(encoding="utf-8", errors="replace"), "run_id": None}


@app.get("/api/logs_tail/{task_id}")
def get_log_tail(
    task_id: str,
    run_id: Optional[str] = None,
    offset: int = 0,
    max_bytes: int = 65536,
    db: Session = Depends(get_db),
):
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")
    if max_bytes <= 0:
        raise HTTPException(status_code=400, detail="max_bytes must be > 0")
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task_dir = _get_task_dir(task)
    run = None
    if run_id:
        run = db.query(Run).filter(Run.id == run_id, Run.task_id == task_id).first()
        if not run:
            return {"data": "", "offset": offset, "eof": True, "run_id": None, "message": "run not found"}
    if not run:
        run = (
            db.query(Run)
            .filter(Run.task_id == task_id)
            .order_by(Run.start_time.desc())
            .first()
        )
    if not run:
        return {"data": "", "offset": offset, "eof": True, "run_id": None, "message": "run not found"}
    log_file = _resolve_run_log_file(task_dir, run.log_path)
    if not log_file.exists():
        return {"data": "", "offset": offset, "eof": True, "run_id": run.id, "message": "log file not found"}
    file_size = log_file.stat().st_size
    if offset >= file_size:
        return {"data": "", "offset": offset, "eof": True, "run_id": run.id}
    read_size = min(max_bytes, file_size - offset)
    with open(log_file, "rb") as f:
        f.seek(offset)
        data = f.read(read_size)
    new_offset = offset + len(data)
    eof = new_offset >= file_size
    return {
        "data": data.decode("utf-8", errors="replace"),
        "offset": new_offset,
        "eof": eof,
        "run_id": run.id,
    }

@app.get("/api/runs/{task_id}")
def list_runs(task_id: str, db: Session = Depends(get_db)):
    runs = (
        db.query(Run)
        .filter(Run.task_id == task_id)
        .order_by(Run.start_time.desc())
        .all()
    )
    return [
        {
            "id": run.id,
            "status": run.status,
            "started_at": run.start_time.strftime("%Y-%m-%d %H:%M:%S") if run.start_time else None,
            "log_path": run.log_path,
        }
        for run in runs
    ]

@app.get("/")
def index(): return FileResponse("static/index.html")

# --- 核心运行建议 ---
# 启动（删除操作不触发 reload）：
#   uvicorn main:app --reload --reload-exclude "tasks_data/*" --reload-exclude "database.db*"
# 若 Ctrl+C 后进程迟迟不退出，可先关闭浏览器中打开的前端页签再试；
# 或缩短 keep-alive：uvicorn main:app --reload --timeout-keep-alive 5 ...
