FROM python:3.12-slim

# python:slim (debian-based), not alpine: duckdb's PyPI wheels are
# built against glibc. Alpine uses musl libc, which would force a
# from-source compile of duckdb at image build time -- slow, and a
# real risk of build failures. slim avoids that entirely while still
# being far smaller than the full python image.
WORKDIR /app
RUN pip install --no-cache-dir duckdb pandas pyarrow

COPY query_api.py ./

EXPOSE 8000
ENTRYPOINT ["python", "query_api.py"]
CMD ["--session-id", "default-session", "--data-dir", "/data", "--port", "8000"]