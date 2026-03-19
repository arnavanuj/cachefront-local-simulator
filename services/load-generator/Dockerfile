FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "python load_test.py --base-url ${TARGET_URL:-http://api:8000} --threads ${THREADS:-20} --requests ${REQUESTS:-500} --user-ids ${USER_IDS:-1,2,3,4,5} --write-every ${WRITE_EVERY:-0}"]
