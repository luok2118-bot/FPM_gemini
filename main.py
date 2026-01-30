# 后端核心代码
# database.db 由程序自动生成
import os
import sys
import subprocess
import datetime
import shutil
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from loguru import logger

# --- 配置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = f"sqlite:///{os.path.join(BASE_DIR, 'database.db')}"

os.makedirs(SCRIPTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# --- 数据库模型 ---
Base = declarative_base()
engine = create_engine(DB_PATH, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class TaskModel(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    script_filename = Column(String)
    conda_env = Column(String)
    cron_expression = Column(String) # 简化：仅存储 "HH:MM" 格式或 cron 字符串
    status = Column(String, default="Idle") # Idle, Running, Failed, Success
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# --- 核心逻辑：执行因子脚本 ---
def run_factor_script(task_id: int, script_name: str, conda_env: str):
    """
    实际执行脚本的函数，将被调度器调用。
    它会通过 subprocess 调用指定的 conda 环境。
    """
    db = SessionLocal()
    task = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    task.status = "Running"
    task.last_run = datetime.datetime.now()
    db.commit()

    # 日志路径: logs/task_id/YYYY-MM-DD.log
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    task_log_dir = os.path.join(LOGS_DIR, str(task_id))
    os.makedirs(task_log_dir, exist_ok=True)
    log_file = os.path.join(task_log_dir, f"{today}.log")

    script_path = os.path.join(SCRIPTS_DIR, script_name)
    
    # 构建 Windows 下的 Conda 调用命令
    # 注意：这里假设 conda 在系统 PATH 中，或者你可以指定 conda.exe 的绝对路径
    cmd = f'conda run -n {conda_env} python -u "{script_path}"'

    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.datetime.now()}] START executing in env: {conda_env}\n")
            f.write("-" * 50 + "\n")
            
            # 使用 subprocess 执行
            process = subprocess.Popen(
                cmd, 
                shell=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, # 将错误重定向到标准输出
                text=True,
                encoding='utf-8',
                errors='replace' # 防止编码报错
            )
            
            # 实时读取输出写入日志
            for line in process.stdout:
                f.write(line)
                
            process.wait()
            
            status = "Success" if process.returncode == 0 else "Failed"
            f.write(f"\n[{datetime.datetime.now()}] END. Status: {status}\n")

    except Exception as e:
        status = "Failed"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\nSYSTEM ERROR: {str(e)}\n")
    
    # 更新数据库状态
    task.status = status
    db.commit()
    db.close()

# --- 调度器设置 ---
jobstores = {
    'default': SQLAlchemyJobStore(url=DB_PATH)
}
scheduler = BackgroundScheduler(jobstores=jobstores)
scheduler.start()

# --- FastAPI 应用 ---
app = FastAPI(title="Factor Calculation Platform")

app.mount("/static", StaticFiles(directory="static"), name="static")

# Pydantic 模型用于前端交互
class TaskCreate(BaseModel):
    name: str
    conda_env: str
    schedule_time: str # 格式 "HH:MM" (每天)

@app.get("/")
def read_root():
    return FileResponse('static/index.html')

@app.get("/api/tasks")
def get_tasks():
    db = SessionLocal()
    tasks = db.query(TaskModel).all()
    # 获取下次运行时间
    result = []
    for t in tasks:
        job = scheduler.get_job(str(t.id))
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job else "Not Scheduled"
        result.append({
            "id": t.id,
            "name": t.name,
            "script": t.script_filename,
            "env": t.conda_env,
            "schedule": t.cron_expression,
            "status": t.status,
            "last_run": t.last_run.strftime("%Y-%m-%d %H:%M:%S") if t.last_run else "-",
            "next_run": next_run
        })
    db.close()
    return result

@app.post("/api/upload")
async def upload_script(file: UploadFile = File(...)):
    file_location = os.path.join(SCRIPTS_DIR, file.filename)
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    return {"filename": file.filename}

@app.post("/api/tasks")
def create_task(name: str = Form(...), script_name: str = Form(...), conda_env: str = Form(...), time: str = Form(...)):
    db = SessionLocal()
    # 创建数据库记录
    new_task = TaskModel(
        name=name,
        script_filename=script_name,
        conda_env=conda_env,
        cron_expression=time
    )
    db.add(new_task)
    db.commit()
    db.refresh(new_task)
    
    # 添加到调度器 (每天指定时间运行)
    hour, minute = time.split(":")
    scheduler.add_job(
        run_factor_script, 
        'cron', 
        hour=int(hour), 
        minute=int(minute), 
        id=str(new_task.id),
        args=[new_task.id, script_name, conda_env],
        replace_existing=True
    )
    
    db.close()
    return {"status": "ok", "task_id": new_task.id}

@app.get("/api/logs/{task_id}")
def get_logs(task_id: int, date: str = None):
    # 默认查看今天的日志
    if not date:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    log_file = os.path.join(LOGS_DIR, str(task_id), f"{date}.log")
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            return {"content": f.read()}
    return {"content": "No log found for this date."}

@app.post("/api/run_now/{task_id}")
def run_now(task_id: int):
    """手动立即触发一次"""
    db = SessionLocal()
    task = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if task:
        scheduler.add_job(run_factor_script, args=[task.id, task.script_filename, task.conda_env])
        return {"status": "triggered"}
    raise HTTPException(status_code=404, detail="Task not found")

@app.get("/api/envs")
def get_conda_envs():
    """简单的获取 conda 环境列表 (通过扫描 conda output)"""
    try:
        # 这里使用简单的方法，实际生产中可能需要解析 conda env list 的 JSON 输出
        output = subprocess.check_output("conda env list", shell=True, text=True)
        envs = []
        for line in output.splitlines():
            if line and not line.startswith("#"):
                parts = line.split()
                if parts:
                    envs.append(parts[0])
        return envs
    except:
        return ["base", "py312", "py38"] # Fallback mock data