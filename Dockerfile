FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir groai-fi-datastore-shared python-dotenv

# Copy service code
COPY . .

CMD ["python", "main.py"]
