# Base: Ubuntu 22.04 (jammy) which Microsoft supports
FROM ubuntu:22.04

# Avoid interactive prompts (e.g. ACEPT_EULA)
ENV DEBIAN_FRONTEND=noninteractive

# Install basics
RUN apt-get update && apt-get install -y \
    curl wget gnupg software-properties-common python3 python3-venv python3-pip unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft SQL ODBC repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && add-apt-repository "deb [arch=amd64] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

#install Python deps
RUN pip install --no-cache-dir kaggle pandas pyarrow sqlalchemy pyodbc python-dotenv

# Make project folder inside container
WORKDIR /app
COPY . /app

# Default command: just drop into bash (you can override with python later)
CMD ["/bin/bash"]