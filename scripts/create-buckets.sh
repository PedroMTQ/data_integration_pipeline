#!/bin/sh
set -e

echo "Setting up MinIO alias..."
until mc alias set "$S3_ALIAS" "$S3_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
  echo "Waiting for MinIO..."
  sleep 2
done

mc admin accesskey list "$S3_ALIAS" 

# Split the env var into an array
IFS=',' read -r -a buckets <<< "$S3_BUCKETS"
echo "Creating buckets $S3_BUCKETS"
for bucket in "${buckets[@]}"; do
  mc mb --ignore-existing "$S3_ALIAS/$bucket"
  echo "Bucket $bucket ensured"
done


echo "Configuring admin access keys..."
if ! mc admin accesskey create "$S3_ALIAS" \
    --access-key "${S3_ACCESS_KEY}" \
    --secret-key "${S3_SECRET_ACCESS_KEY}"; then
    echo "Failed to create access key (exit code $?), it probably already exists."
fi

echo "MinIO setup complete!"