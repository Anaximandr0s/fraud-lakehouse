ARG PYTHON_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE}
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
RUN uv sync --locked --no-dev --group producer --no-editable
COPY producer/ producer/
# Created in the image so the named volume mounted on /state is owned by the producer user.
RUN useradd --system --no-create-home --uid 10001 producer && mkdir /state && chown producer /state
USER 10001
CMD ["/app/.venv/bin/python", "producer/main.py"]
