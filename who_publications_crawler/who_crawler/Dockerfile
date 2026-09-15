FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY who_crawler/ ./who_crawler/

# Non-root user for security compliance (Security & Governance workstream)
RUN useradd -m crawler
USER crawler

ENV PYTHONUNBUFFERED=1
EXPOSE 9090

ENTRYPOINT ["python", "-m", "who_crawler.main"]
CMD ["--source", "who_publications"]
