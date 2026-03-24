FROM astral/uv:0.9.2-python3.14-alpine

RUN addgroup -g 1000 appgroup && \
    adduser -D -u 1000 -G appgroup appuser

WORKDIR /app
RUN chown appuser:appgroup /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev
RUN chown -R appuser:appgroup /app/.venv

COPY --chown=appuser . /app

USER appuser

CMD ["uv", "run", "python", "-m", "kvstore_server"]