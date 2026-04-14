FROM python:3.12-slim

WORKDIR /app

# Install uv — needed because groai-fi-datastore-shared uses uv_build
# as its build backend, which plain pip cannot bootstrap.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies declared in pyproject.toml.
# price-oracle-lf is a service (not a library), so we install deps directly
# rather than building a wheel.
COPY pyproject.toml .
RUN uv pip install --system --no-cache \
    "groai-fi-datastore-shared>=0.2.4" \
    "python-dotenv>=1.0.0"

# Copy source after deps are cached
COPY . .

CMD ["python", "main.py"]
