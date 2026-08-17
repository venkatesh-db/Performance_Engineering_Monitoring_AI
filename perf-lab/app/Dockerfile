FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    fastapi==0.115.0 uvicorn==0.30.6 asyncpg==0.29.0 \
    redis==5.0.8 pydantic==2.9.0 py-spy==0.3.14
COPY main.py .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
