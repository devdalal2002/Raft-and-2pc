# Dockerfile for Python gRPC queue service
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. queue.proto
# Ensure test-runner.sh is executable
RUN chmod +x /app/test-runner.sh
RUN chmod +x /app/entrypoint.sh
EXPOSE 50051
ENTRYPOINT ["python"]
CMD ["server.py"]
