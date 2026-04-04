FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (no dev deps, frozen lockfile)
RUN uv sync --frozen --no-dev

# Copy application
COPY server.py .

EXPOSE 5000

CMD ["uv", "run", "python", "server.py"]
