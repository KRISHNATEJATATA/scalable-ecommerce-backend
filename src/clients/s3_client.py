"""Async S3 client (aioboto3).

Single async upload/serve path. MinIO locally via ``S3_ENDPOINT_URL``; real S3
in the cloud using the ECS task role (no keys in code). Wired in Phase 7.
"""
