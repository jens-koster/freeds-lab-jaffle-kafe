FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml requirements.txt README.md /app/
COPY src /app/src

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir .

RUN mkdir -p /opt/freeds/config /opt/data

CMD ["python3", "-m", "jafkafe.simulator"]
