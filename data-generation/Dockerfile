FROM python:3.9-slim

WORKDIR /app

# Install required packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the data generation script
COPY generate_dummy_data.py .

# Create data directory
RUN mkdir -p /app/data

# Generate data when container starts
CMD ["python", "generate_dummy_data.py"]
