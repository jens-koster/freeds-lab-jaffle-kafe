FROM python:3.11-slim


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/freeds/config
RUN mkdir -p /opt/data
COPY src/jafkafe jafkafe

CMD ["python3", "jafkafe/simulator.py"]
