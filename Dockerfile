# ============================================================
# 证据链侦探 · 轻量化演示部署镜像
# 构建：docker build -t detective-app .
# 运行：docker run -d -p 8000:8000 \
#         -e WSA_API_KEY=xxx -e DEEPSEEK_API_KEY=xxx detective-app
# ============================================================
FROM python:3.11-slim

# 避免 pyc、缓冲输出直接进日志
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖，利用 Docker 层缓存
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# 复制后端与前端静态资源
COPY backend /app/backend
COPY frontend /app/frontend

WORKDIR /app/backend
EXPOSE 8000

# 演示环境无需 --reload；多 worker 会破坏内存态进度表，保持单进程
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
