# Dockerfile
# Start from an official Python 3.10 image
# 'slim' means a minimal Linux install — smaller image, faster to download
FROM python:3.10-slim

# Set the working directory inside the container
# All subsequent commands run from here
WORKDIR /app

# Copy requirements first — before the rest of the code
# Docker caches each step. If requirements.txt hasn't changed,
# it skips the pip install step on rebuilds — much faster
COPY requirements.txt .

# Install dependencies
# --no-cache-dir keeps the image smaller
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the code
# Done after pip install so code changes don't invalidate the pip cache
COPY pipeline/   ./pipeline/
COPY tests/      ./tests/
COPY run_pipeline.py .

# Create the output directory inside the container
RUN mkdir -p output

# Default command: run the pipeline
# This is what executes when you run: docker run <image>
CMD ["sh", "-c", "pytest tests/ -v && python run_pipeline.py"]
