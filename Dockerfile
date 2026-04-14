FROM python:3.12-slim

WORKDIR /app

# Install dependencies pulling from pyproject.toml
COPY . .
RUN pip install --no-cache-dir .

CMD ["python", "main.py"]
