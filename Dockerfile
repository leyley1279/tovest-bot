FROM python:3.11-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy requirements và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Tạo thư mục data cho SQLite (persistent volume)
RUN mkdir -p /data

# Biến môi trường
ENV DB_PATH=/data/bot_data.db
ENV PYTHONUNBUFFERED=1

# Chạy bot
CMD ["python", "bot.py"]
