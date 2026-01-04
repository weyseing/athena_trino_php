# Setup Guide
- **Create IAM User with permission below**
    - `AmazonAthenaFullAccess`
    - `AWSGlueConsoleFullAccess`
    - `AmazonS3FullAccess`

- **Copy `.env.example` to `.env` and fill up values below**
```properties
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

- **Create S3 bucket** for query result
    - Update to `.env`
```properties
ATHENA_OUTPUT_S3_PATH=s3://<S3_BUCKET>/athena-results/
```

- **Composer install**
```bash
docker exec -it athena_trino_php bash
composer install
```


