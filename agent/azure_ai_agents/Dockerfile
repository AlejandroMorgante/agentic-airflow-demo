FROM python:3.12-slim

WORKDIR /app

COPY azure_ai_agents/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY azure_ai_agents/agent.py .
COPY shared ./shared

EXPOSE 8088

CMD ["python", "agent.py"]
