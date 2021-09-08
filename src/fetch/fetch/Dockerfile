FROM python:3-alpine

COPY fetch.py .
COPY catalog.yml .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN apk add curl wget

CMD ["python3", "-u", "fetch.py"]
