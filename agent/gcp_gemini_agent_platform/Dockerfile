FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY gcp_gemini_agent_platform/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY gcp_gemini_agent_platform/agent.py .

EXPOSE 8080

CMD ["python", "agent.py"]
