FROM python:3.11-slim

WORKDIR /app
COPY ../../consensus /app/consensus
RUN pip install --no-cache-dir -r consensus/requirements.txt

ENV PYTHONPATH=/app
CMD ["python", "-m", "consensus.two_pc.node"]
