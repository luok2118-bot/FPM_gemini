import os
import uuid
import queue
import asyncio
import subprocess
import threading
import datetime
import shutil
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine, Column, String, DateTime, event
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
    script_name = Column(String)
    conda_env = Column(String)
    cron_time = Column(String)
    upstream_id = Column(String, nullable=True)
    status = Column(String, default="Idle")
    last_run = Column(DateTime, nullable=True)

class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True)
    task_id = Column(String, index=True)
    status = Column(String, default="Running")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    log_path = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# --- 事件推送逻辑 ---
_event_queue = queue.Queue()
_sse_queues: set = set()
_client_sse_queues: dict[str, asyncio.Queue] = {}

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

# --- 任务并发控制：每个 task_id 只允许一个实例在跑 ---
_running_tasks: set = set()
_running_lock = threading.Lock()

def execute_factor_task(task_id: str):
    with _running_lock:
        if task_id in _running_tasks:
            return  # 已有实例在跑，跳过
        _running_tasks.add(task_id)
    try:
        _do_execute_task(task_id)
    finally:
        with _running_lock:
            _running_tasks.discard(task_id)

def _do_execute_task(task_id: str):
    db = SessionLocal()
    try:
        task = db.query(Task).filter(Task.id == task_id).first()
        if not task: return

        task_dir = DATA_ROOT / task_id
        script_path = task_dir / "script" / task.script_name
        env_vars = os.environ.copy()
        env_vars.update({
            "TASK_ID": task_id,
            "OUTPUT_PATH": str(task_dir / "output"),
            "UPSTREAM_OUTPUT_PATH": str(DATA_ROOT / task.upstream_id / "output") if task.upstream_id else ""
        })

        now = datetime.datetime.now()
        run_id = str(uuid.uuid4())
        log_dir = task_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        log_filename = f"{now.strftime('%Y%m%d_%H%M%S')}_{run_id}.log"
        run = Run(
            id=run_id,
            task_id=task_id,
            status="Running",
            started_at=now,
            log_path=str(Path("logs") / log_filename),
        )
        db.add(run)

        task.status = "Running"
        task.last_run = now
        db.commit()

        log_file = task_dir / "logs" / log_filename
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*20} START {datetime.datetime.now()} {'='*20}\n")
            process = subprocess.Popen(
                f'conda run -n {task.conda_env} python -u "{script_path}"',
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', env=env_vars
            )
            for line in process.stdout: f.write(line)
            process.wait()
            task.status = "Success" if process.returncode == 0 else "Failed"
            run.status = task.status
            run.finished_at = datetime.datetime.now()
    finally:
        db.commit()
        db.close()
        _event_queue.put_nowait("update")

# --- 调度器初始化 ---
scheduler = BackgroundScheduler(jobstores={'default': SQLAlchemyJobStore(engine=engine)})
scheduler.start()

# --- FastAPI 接口 ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(BASE_DIR / "static" / "favicon.ico")

@app.on_event("startup")
async def startup():
    # 将上次崩溃时残留的 Running 任务置为 Unknown，由使用者看日志排查
    db = SessionLocal()
    try:
        db.query(Task).filter(Task.status == "Running").update({"status": "Unknown"}, synchronize_session=False)
        db.commit()
    finally:
        db.close()
    asyncio.create_task(_sse_broadcast_loop())

@app.on_event("shutdown")
def on_shutdown():
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
        res.append({
            "id": t.id, "name": t.name, "script": t.script_name,
            "env": t.conda_env, "schedule": t.cron_time, "status": t.status,
            "last_run": t.last_run.strftime("%Y-%m-%d %H:%M:%S") if t.last_run else "-",
            "next_run": job.next_run_time.strftime("%H:%M:%S") if job and job.next_run_time else "Paused"
        })
    return res

@app.post("/api/tasks")
async def create_task(
    name: str = Form(...), conda_env: str = Form(...), time: str = Form(...),
    upstream_id: Optional[str] = Form(None), file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    task_id = str(uuid.uuid4())
    task_dir = DATA_ROOT / task_id
    for sub in ["script", "logs", "output"]: (task_dir / sub).mkdir(parents=True)
    
    with open(task_dir / "script" / file.filename, "wb") as f: f.write(await file.read())

    new_task = Task(id=task_id, name=name, script_name=file.filename, conda_env=conda_env, cron_time=time, upstream_id=upstream_id)
    db.add(new_task); db.commit()
    
    h, m = time.split(":")
    scheduler.add_job(execute_factor_task, 'cron', hour=int(h), minute=int(m), id=task_id, args=[task_id])
    _event_queue.put_nowait("changed")
    return {"id": task_id}

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str, bg: BackgroundTasks, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # 1. 立即从数据库和调度器中移除
    db.delete(task); db.commit()
    if scheduler.get_job(task_id): scheduler.remove_job(task_id)
    
    # 2. 物理删除交给后台，不阻塞 API 响应，防止死锁
    bg.add_task(shutil.rmtree, DATA_ROOT / task_id, ignore_errors=True)
    
    _event_queue.put_nowait("changed")
    return {"status": "success"}

@app.post("/api/run_now/{task_id}")
def run_now(task_id: str, db: Session = Depends(get_db)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task.status = "Running"
    task.last_run = datetime.datetime.now()
    db.commit()
    scheduler.add_job(execute_factor_task, args=[task_id], id=f"{task_id}_manual")
    _event_queue.put_nowait("changed")
    return {"status": "triggered"}

@app.get("/api/logs/{task_id}")
def get_log(task_id: str, run_id: Optional[str] = None, db: Session = Depends(get_db)):
    log_dir = DATA_ROOT / task_id / "logs"
    if not log_dir.exists():
        return {"content": "暂无日志数据。", "run_id": None}
    if run_id:
        run = db.query(Run).filter(Run.id == run_id, Run.task_id == task_id).first()
        if not run:
            return {"content": "未找到对应运行记录。", "run_id": None}
        log_file = Path(run.log_path or "")
        if not log_file.is_absolute():
            log_file = DATA_ROOT / task_id / log_file
        if log_file.exists():
            return {"content": log_file.read_text(encoding="utf-8", errors="replace"), "run_id": run.id}
        return {"content": "日志文件不存在。", "run_id": run.id}

    run = db.query(Run).filter(Run.task_id == task_id).order_by(Run.started_at.desc()).first()
    if run:
        log_file = Path(run.log_path or "")
        if not log_file.is_absolute():
            log_file = DATA_ROOT / task_id / log_file
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
        .order_by(Run.started_at.desc())
        .all()
    )
    return [
        {
            "id": run.id,
            "status": run.status,
            "started_at": run.started_at.strftime("%Y-%m-%d %H:%M:%S") if run.started_at else None,
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
