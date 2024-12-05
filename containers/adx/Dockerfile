FROM python:3.11-bookworm

# Set the working directory in the container
WORKDIR /

# Copy the Python script into the container
COPY main.py .

# Install required dependencies
RUN pip install azure-identity kubernetes azure-mgmt-kusto azure-mgmt-eventhub azure-kusto-data azure-mgmt-authorization

# Run the Python script
CMD ["python", "main.py"]