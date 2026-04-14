FROM python:3.12-slim

WORKDIR /app

# Install uv — needed because groai-fi-datastore-shared uses uv_build
# as its build backend, which plain pip cannot bootstrap.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies pulling from pyproject.toml
COPY . .
RUN uv pip install --system --no-cache .

CMD ["python", "main.py"]
