#!/bin/bash

# Test MinIO setup script

echo "🚀 Testing MinIO setup..."

# Check if MinIO is running
if ! curl -s http://localhost:9000/minio/health/live > /dev/null; then
  echo "❌ MinIO is not running. Please start the services with 'docker-compose up -d'"
  exit 1
fi

echo "✅ MinIO is running"

# Check if we can connect to MinIO
if ! mc alias list | grep -q myminio; then
  echo "Configuring MinIO client..."
  mc alias set myminio http://localhost:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin}
fi

echo "✅ MinIO client configured"

# Check if the bucket exists
BUCKET=${MINIO_DEFAULT_BUCKETS:-map-tiles}
if mc ls myminio | grep -q "$BUCKET"; then
  echo "✅ Bucket '$BUCKET' exists"
  echo "Testing bucket permissions..."
  
  # Create a test file
  echo "test" > test.txt
  
  # Upload test file
  if mc cp test.txt myminio/$BUCKET/; then
    echo "✅ Successfully uploaded test file to bucket"
    
    # List files in bucket
    echo "📂 Files in bucket:"
    mc ls myminio/$BUCKET/
    
    # Clean up
    mc rm myminio/$BUCKET/test.txt
    rm test.txt
    echo "✅ Cleaned up test files"
  else
    echo "❌ Failed to upload test file to bucket"
    exit 1
  fi
else
  echo "❌ Bucket '$BUCKET' does not exist"
  exit 1
fi

echo "\n🎉 MinIO setup test completed successfully!"
