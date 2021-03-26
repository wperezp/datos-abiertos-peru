FROM python:3-alpine
COPY standalone_fetch.py .
COPY catalog.yml .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python3", "-u", "standalone_fetch.py", "catalog.yml"]