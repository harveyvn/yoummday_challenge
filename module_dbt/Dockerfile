FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY app/ /opt/app/
WORKDIR /opt/app

CMD ["dbt", "debug", "--profiles-dir", "profiles"]
