# Map Tile Downloader

A modular Python application for downloading map tiles from various sources with support for rate limiting, IP rotation, and multiple output formats.

## Features

- Multiple download strategies (rate limiting, time-based, etc.)
- IP pool support for distributed downloading
- Configurable via YAML
- Support for MBTiles format
- MinIO integration for cloud storage
- Temporary file management with cleanup
- Containerized with Docker

## Prerequisites

- Python 3.7+
- Docker and Docker Compose (for containerized deployment)
- Git (for cloning the repository)

## Installation

### Local Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd map-tile-downloader
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Docker Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd map-tile-downloader
   ```

2. Build the Docker image:
   ```bash
   docker-compose build
   ```

## Configuration

1. Copy the example configuration file:
   ```bash
   cp config/config.example.yaml config/config.yaml
   ```

2. Edit `config/config.yaml` to set up your download jobs, output destinations, and other parameters.

## Usage

### Local Usage

```bash
python -m map_downloader --config config/config.yaml
```

### Docker Usage

1. Start MinIO and the application:
   ```bash
   docker-compose up -d
   ```

2. Access the MinIO web interface at http://localhost:9001
   - Username: minioadmin
   - Password: minioadmin

3. To run a specific download job:
   ```bash
   docker-compose run --rm app python -m map_downloader --config /app/config/config.yaml
   ```

4. To access the container's shell:
   ```bash
   docker-compose run --rm app /bin/bash
   ```

## Configuration Reference

### MinIO Configuration

When using the provided Docker Compose setup, MinIO is pre-configured with the following settings:
- Endpoint: `http://minio:9000` (from within the Docker network)
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Default Bucket: `map-tiles`

### Environment Variables

You can override the following environment variables in the `docker-compose.yml` file:

- `MINIO_ROOT_USER`: MinIO root username (default: `minioadmin`)
- `MINIO_ROOT_PASSWORD`: MinIO root password (default: `minioadmin`)
- `MINIO_DEFAULT_BUCKETS`: Comma-separated list of buckets to create (default: `map-tiles`)

## Data Persistence

- MinIO data is persisted in a Docker volume named `minio_data`
- Downloaded tiles are stored in the `./data` directory by default

## Troubleshooting

- If you encounter permission issues with MinIO, check the container logs:
  ```bash
  docker-compose logs minio
  ```

- To reset the MinIO data (warning: this will delete all data):
  ```bash
  docker-compose down -v
  docker-compose up -d
  ```
