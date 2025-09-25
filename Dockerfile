# 使用Python 3.11基础镜像（因为你的依赖包兼容性更好）
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 创建数据和日志目录
RUN mkdir -p /app/data /app/logs

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 更新源列表并更换为阿里云源
RUN echo 'deb http://mirrors.aliyun.com/debian/ bookworm main' > /etc/apt/sources.list && \
    echo 'deb-src http://mirrors.aliyun.com/debian/ bookworm main' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/debian/ bookworm-updates main' >> /etc/apt/sources.list && \
    echo 'deb-src http://mirrors.aliyun.com/debian/ bookworm-updates main' >> /etc/apt/sources.list && \
    echo 'deb http://mirrors.aliyun.com/debian-security bookworm-security main' >> /etc/apt/sources.list && \
    echo 'deb-src http://mirrors.aliyun.com/debian-security bookworm-security main' >> /etc/apt/sources.list

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制requirements.txt
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 暴露端口（假设Flask应用运行在5000端口）
EXPOSE 8888

# 使用gunicorn启动应用
CMD ["gunicorn", "--bind", "0.0.0.0:8888", "--workers", "4", "web_server:app"] 