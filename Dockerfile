FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY server.py .

# Install dependencies using uv
RUN uv sync

# Create data volume directory
RUN mkdir -p /data
VOLUME ["/data"]

# Set environment variable for files path
ENV FILES_PATH=/data

# Run the server using uv
CMD ["uv", "run", "server.py"]