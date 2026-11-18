FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt
COPY contracts contracts
COPY src src
COPY tests tests
COPY scripts scripts

CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]
