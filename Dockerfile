# Use Python 3.8 slim image
FROM python:3.8-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY . .

# Create a non-root user
RUN useradd --create-home --shell /bin/bash app
USER app

# Expose port (Railway will set this)
EXPOSE 5000

# Start the bot
CMD ["python", "bot.py"]
