# Use a stable Python base image
FROM python:3.10-slim-bookworm

# Set environment variables for stability
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=on

# Set the working directory
WORKDIR /app

# Copy requirements file first to leverage Docker's build cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the default Streamlit port
EXPOSE 8501

# The command to run the Streamlit application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]