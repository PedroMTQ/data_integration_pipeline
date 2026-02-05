FROM python:3.11.3-bullseye AS build

# installs uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

WORKDIR "/app"
COPY ./src/ /app/src/
COPY ./uv.lock /app/uv.lock
COPY ./pyproject.toml /app/pyproject.toml
COPY ./README.md /app/README.md

# Extract version and add to a __version__ file which will be read by the service later
RUN echo $(grep -m 1 'version' /app/pyproject.toml | sed -E 's/version = "(.*)"/\1/') > /app/__version__

RUN echo "Installing package"
# installs package
RUN uv sync --frozen
ENV PATH="/app/.venv/bin:$PATH"

# Final
FROM build
RUN apt autoremove -y && apt clean -y

