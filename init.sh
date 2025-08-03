#!/bin/bash

# Initialize the Map Tile Downloader environment

# Exit on error
set -e

echo "🚀 Setting up Map Tile Downloader..."

# Create necessary directories
echo "📂 Creating directories..."
mkdir -p config data/temp

# Copy example config if it doesn't exist
if [ ! -f "config/config.yaml" ]; then
    echo "📄 Creating default configuration..."
    cp config/config.example.yaml config/config.yaml
    
    # Update MinIO configuration in the config file
    if [ -f "config/config.yaml" ]; then
        echo "🔧 Updating MinIO configuration..."
        sed -i 's/endpoint:.*/endpoint: "http:\/\/minio:9000"/' config/config.yaml
        sed -i 's/access_key:.*/access_key: "'${MINIO_ROOT_USER:-minioadmin}'"/' config/config.yaml
        sed -i 's/secret_key:.*/secret_key: "'${MINIO_ROOT_PASSWORD:-minioadmin}'"/' config/config.yaml
        sed -i 's/bucket_name:.*/bucket_name: "'${MINIO_DEFAULT_BUCKETS:-map-tiles}'"/' config/config.yaml
    fi
else
    echo "ℹ️  Configuration file already exists, skipping..."
fi

# Set permissions
echo "🔒 Setting permissions..."
chmod -R 777 data

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file..."
    cp .env.example .env
else
    echo "ℹ️  .env file already exists, skipping..."
fi

echo "✅ Setup complete! You can now start the application with:"
echo "   docker-compose up -d"
echo ""
echo "Access MinIO console at: http://localhost:9001"
echo "Default credentials:"
echo "- Username: ${MINIO_ROOT_USER:-minioadmin}"
echo "- Password: ${MINIO_ROOT_PASSWORD:-minioadmin}"
