-- Database "metastore" và user "hive" đã được tạo tự động từ env var.
-- Script này tạo user + database riêng cho Airflow để tách biệt quyền truy cập.

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
