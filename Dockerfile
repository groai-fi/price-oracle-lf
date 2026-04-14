FROM python:3.12-slim

WORKDIR /app

# Install uv — needed because groai-fi-datastore-shared uses uv_build
# as its build backend, which plain pip cannot bootstrap.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy pyproject.toml and uv.lock to guarantee exact versions
COPY pyproject.toml uv.lock ./

# Install dependencies using exact locked versions. 
# This automatically creates a virtual environment (.venv) inside the container.
RUN uv sync --locked --no-dev --no-cache

# Copy the rest of the source code
COPY . .

# Run via uv run, which seamlessly routes execution through the .venv created above
CMD ["uv", "run", "main.py"]
