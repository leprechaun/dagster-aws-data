FROM python:3.14-slim


# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install --upgrade uv

# Install system-wide (into /usr/local) instead of a project-local .venv,
# while still honoring uv.lock via --frozen so the image matches what CI tested.
ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Copy your Dagster project. You may need to replace the filepath depending on your project structure
WORKDIR /deploy_k8s/

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . /deploy_k8s/
RUN uv sync --frozen --no-dev
