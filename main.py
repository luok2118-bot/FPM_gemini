import os
import uuid
import json
import subprocess
import datetime
import shutil
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine, Column, String, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from loguru import logger

# --- 基础配置 ---
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = BASE_DIR / "tasks_data"
DATA_ROOT.mkdir(exist_ok=True)
DB_PATH = f"sqlite:///{BASE_DIR}/database.db"

# --- 数据库定义 ---
Base = declarative_base()
engine = create_engine(DB_PATH, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True)  # UUID
    name = Column(String)
    script_name = Column(String)
    conda_env = Column(String)
    cron_time = Column(String)
    upstream_id = Column(String, nullable=True)
    status = Column(String, default="Idle")
    last_run = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# --- 核心调度逻辑 ---
def execute_factor_task(task_id: str):
    db = SessionLocal()
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task: return

    # 1. 准备路径
    task_dir = DATA_ROOT / task_id
    script_path = task_dir / "script" / task.script_name
    log_dir = task_dir / "logs"
    output_dir = task_dir / "output"
    
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"{today}.log"
    
    # 2. 更新状态
    task.status = "Running"
    task.last_run = datetime.datetime.now()
    db.commit()

    # 3. 环境变量注入：让脚本知道自己在哪，以及上游在哪
    env_vars = os.environ.copy()
    env_vars["TASK_ID"] = task_id
    env_vars["OUTPUT_PATH"] = str(output_dir)
    if task.upstream_id:
        env_vars["UPSTREAM_OUTPUT_PATH"] = str(DATA_ROOT / task.upstream_id / "output")
    else:
        env_vars["UPSTREAM_OUTPUT_PATH"] = ""

    # 4. 执行命令
    cmd = f'conda run -n {task.conda_env} python -u "{script_path}"'
    
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*20} [{datetime.datetime.now()}] START {'='*20}\n")
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', errors='replace', env=env_vars
            )
            for line in process.stdout:
                f.write(line)
            process.wait()
            task.status = "Success" if process.returncode == 0 else "Failed"
    except Exception as e:
        task.status = "Failed"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\nSYSTEM ERROR: {str(e)}\n")
    finally:
        db.commit()
        db.close()

# --- 调度器初始化 ---
scheduler = BackgroundScheduler(jobstores={'default': SQLAlchemyJobStore(url=DB_PATH)})
scheduler.start()

# --- FastAPI 接口 ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index(): return FileResponse("static/index.html")

@app.get("/api/envs")
def get_envs():
    try:
        output = subprocess.check_output("conda env list", shell=True, text=True)
        return [line.split()[0] for line in output.splitlines() if line and not line.startswith("#")]
    except: return ["base"]

@app.get("/api/tasks")
def list_tasks():
    db = SessionLocal()
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
    db.close()
    return res

@app.post("/api/tasks")
async def create_task(
    name: str = Form(...),
    conda_env: str = Form(...),
    time: str = Form(...),
    upstream_id: Optional[str] = Form(None),
    file: UploadFile = File(...)
):
    task_id = str(uuid.uuid4())
    
    # 1. 创建任务文件夹
    task_dir = DATA_ROOT / task_id
    (task_dir / "script").mkdir(parents=True)
    (task_dir / "logs").mkdir(parents=True)
    (task_dir / "output").mkdir(parents=True)
    
    # 2. 保存脚本文件
    script_path = task_dir / "script" / file.filename
    with open(script_path, "wb") as f:
        f.write(await file.read())
        
    # 3. 记录元数据
    config = {
        "id": task_id, "name": name, "env": conda_env, 
        "upstream": upstream_id, "created_at": str(datetime.datetime.now())
    }
    with open(task_dir / "config.json", "w") as f:
        json.dump(config, f)

    # 4. 存入数据库
    db = SessionLocal()
    new_task = Task(
        id=task_id, name=name, script_name=file.filename,
        conda_env=conda_env, cron_time=time, upstream_id=upstream_id
    )
    db.add(new_task)
    db.commit()
    
    # 5. 添加定时任务
    h, m = time.split(":")
    scheduler.add_job(execute_factor_task, 'cron', hour=int(h), minute=int(m), id=task_id, args=[task_id])
    db.close()
    return {"id": task_id}

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str):
    db = SessionLocal()
    try:
        # 1. 从数据库移除
        task = db.query(Task).filter(Task.id == task_id).first()
        if not task:
            return {"status": "error", "message": "Task not found"}
        db.delete(task)
        db.commit()

        # 2. 移除定时任务
        if scheduler.get_job(task_id):
            scheduler.remove_job(task_id)

        # 3. 物理删除文件夹
        task_dir = DATA_ROOT / task_id
        if task_dir.exists():
            shutil.rmtree(task_dir)
            
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        db.close()

@app.get("/api/logs/{task_id}")
def get_log(task_id: str, date: str):
    log_file = DATA_ROOT / task_id / "logs" / f"{date}.log"
    if log_file.exists():
        return {"content": log_file.read_text(encoding="utf-8", errors="replace")}
    return {"content": "No log entry for this date."}

@app.post("/api/run_now/{task_id}")
def run_now(task_id: str):
    scheduler.add_job(execute_factor_task, args=[task_id], id=f"{task_id}_manual")
    return {"status": "triggered"}