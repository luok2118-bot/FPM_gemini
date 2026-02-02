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
        OUTPUT_RETENTION_DAYS,
    )
except ImportError:
    QUEUE_WORKER_CONCURRENCY = 1
    QUEUE_EVAL_INTERVAL_SEC = 3
    LOG_RETENTION_DAYS = 7
    OUTPUT_RETENTION_DAYS = 30

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


def _make_output_dir(task_dir: Path, start_time: datetime.datetime) -> Path:
    """基于时间戳生成输出目录，避免重名。"""
    base_name = start_time.strftime("%Y%m%d_%H%M%S")
    output_root = task_dir / "output"
    candidate = output_root / base_name
    suffix = 1
    while candidate.exists():
        suffix += 1
        candidate = output_root / f"{base_name}_{suffix}"
    return candidate


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
    """清理输出文件夹：行情更新只保留最新；因子任务按保留天数清理。"""
    output_root = task_dir / "output"
    if not output_root.exists():
        return
    if (task.task_type or "factor") == "market":
        if current_output is None:
            return
        for path in output_root.iterdir():
            if current_output and path.resolve() == current_output.resolve():
                continue
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
        return
    retention_days = max(int(OUTPUT_RETENTION_DAYS or 0), 0)
    if retention_days <= 0:
        return
    cutoff = time.time() - retention_days * 86400
    for path in output_root.iterdir():
        try:
            if path.is_dir() and path.stat().st_mtime < cutoff:
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass


def enqueue_job(task_id: str, trigger_type: str = "cron"):
    """将任务入队：插入 job_queue 一条 status=Pending、run_date=today 的记录。"""
    db = SessionLocal()
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        if not task:
            return
        now = datetime.datetime.now()
        run_date = datetime.date.today()
        if trigger_type == "cron" and not _is_trading_day(run_date) and not bool(task.run_on_non_trading or 0):
            return
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
    finally:
        db.close()


def _upstream_success_for_date(db: Session, upstream_id: str, run_date: datetime.date) -> bool:
    """是否存在 task_id=upstream_id、run_date、status=Success 的 Run。"""
    r = (
        db.query(Run)
        .filter(
            Run.task_id == upstream_id,
            Run.run_date == run_date,
            Run.status == "Success",
        )
        .first()
    )
    return r is not None


def evaluate_queue_item(db: Session, job: JobQueue) -> str:
    """
    评估单条队列表项：若依赖满足则返回 'Runnable'，否则返回 'Wait'。
    不写库，仅返回目标状态。
    """
    task = db.query(Task).filter(Task.id == job.task_id).first()
    if not task:
        return "Wait"
    if not task.upstream_id:
        return "Runnable"
    if _upstream_success_for_date(db, task.upstream_id, job.run_date):
        return "Runnable"
    return "Wait"


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
    script_path = task_dir / "script" / task.script_name
    run_output_dir = Path(run.output_path) if run.output_path else _make_output_dir(task_dir, run.start_time)
    run_output_dir = run_output_dir if run_output_dir.is_absolute() else task_dir / run_output_dir
    if not run.output_path:
        run.output_path = str(run_output_dir)
    run_output_dir.mkdir(parents=True, exist_ok=True)
    log_path = run.log_path
    log_file = task_dir / log_path if not Path(log_path or "").is_absolute() else Path(log_path)

    upstream_output_path = ""
    if task.upstream_id:
        upstream_run = (
            db.query(Run)
            .filter(
                Run.task_id == task.upstream_id,
                Run.status == "Success",
                Run.run_date == run_date,
            )
            .order_by(Run.start_time.desc())
            .first()
        )
        if not upstream_run:
            return "Wait"
        upstream_output_path = upstream_run.output_path or ""

    env_vars = os.environ.copy()
    env_vars.update({
        "TASK_ID": task_id,
        "OUTPUT_PATH": str(run_output_dir),
        "UPSTREAM_OUTPUT_PATH": upstream_output_path,
    })
    start_time = run.start_time
    exit_code = None
    status = "Failed"
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*20} START {datetime.datetime.now()} {'='*20}\n")
            process = subprocess.Popen(
                f'conda run -n {task.conda_env} python -u "{script_path}"',
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", env=env_vars,
                start_new_session=True,
            )
            with _task_process_lock:
                _task_processes[task_id] = process
            for line in process.stdout:
                f.write(line)
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
    _cleanup_task_outputs(task, task_dir, run_output_dir if status == "Success" else None)
    _event_queue.put_nowait("update")
    return status


def _claim_runnable_job(db: Session):
    """
    认领一条 Runnable 且同一 task_id 无 Running 的队列表项。
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
    candidate = next((j for j in candidates if j.task_id not in running_task_ids), None)
    if not candidate:
        return None, False
    now = datetime.datetime.now()
    rows = (
        db.query(JobQueue)
        .filter(JobQueue.id == candidate.id, JobQueue.status == "Runnable")
        .update({"status": "Running", "updated_at": now}, synchronize_session=False)
    )
    db.commit()
    if rows == 0:
        return None, False
    db.refresh(candidate)
    return candidate, True


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
            if not _upstream_success_for_date(db, task.upstream_id, run_date) if task.upstream_id else False:
                job.status = "Wait"
                job.updated_at = datetime.datetime.now()
                db.commit()
                db.close()
                _event_queue.put_nowait("update")
                continue
            start_time = datetime.datetime.now()
            run_id = str(uuid.uuid4())
            task_dir = _get_task_dir(task)
            run_output_dir = _make_output_dir(task_dir, start_time)
            run_output_dir.mkdir(parents=True, exist_ok=True)
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
                output_path=str(run_output_dir),
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
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except Exception:
                process.kill()
        job.status = "Stopped"
        job.updated_at = datetime.datetime.now()
        task = db.query(Task).filter(Task.id == task_id).first()
        if task:
            task.status = "Stopped"
        db.commit()
        _event_queue.put_nowait("update")
        return {"status": "stopped"}
    raise HTTPException(status_code=400, detail="Job already finished")


@app.post("/api/run_now/{task_id}")
def run_now(task_id: str, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    enqueue_job(task_id, "manual")
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
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except Exception:
        process.kill()
    task.status = "Stopped"
    db.commit()
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
        log_file = Path(run.log_path or "")
        if not log_file.is_absolute():
            log_file = task_dir / log_file
        if log_file.exists():
            return {"content": log_file.read_text(encoding="utf-8", errors="replace"), "run_id": run.id}
        return {"content": "日志文件不存在。", "run_id": run.id}

    run = db.query(Run).filter(Run.task_id == task_id).order_by(Run.start_time.desc()).first()
    if run:
        log_file = Path(run.log_path or "")
        if not log_file.is_absolute():
            log_file = task_dir / log_file
        if log_file.exists():
            return {"content": log_file.read_text(encoding="utf-8", errors="replace"), "run_id": run.id}
        return {"content": "日志文件不存在。", "run_id": run.id}

    log_files = list(log_dir.glob("*.log"))
    if not log_files:
        return {"content": "暂无日志数据。", "run_id": None}
    latest = max(log_files, key=lambda p: p.stat().st_mtime)
    return {"content": latest.read_text(encoding="utf-8", errors="replace"), "run_id": None}

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
