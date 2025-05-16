# FraudIQ

A modular real-time financial data platform for fraud detection and transaction reconciliation.

## Overview

FraudIQ is a scalable data platform that:
- Ingests transaction data streams from multiple sources (banks, gateways, processors)
- Performs real-time fraud detection using pattern matching
- Reconciles transactions across different sources
- Extracts features for fraud analysis
- Stores data in a versioned, queryable format

## Architecture

The platform consists of:
- **Apache Kafka**: Data ingestion and streaming (v3.4.0)
- **Apache Flink**: Real-time complex event processing for fraud detection (v1.17.0)
- **Apache Spark**: Batch processing for reconciliation and feature extraction (v3.4.0)
- **Apache Airflow**: Workflow orchestration (v2.6.3)
- **MinIO**: S3-compatible object storage
- **Docker Compose**: Container orchestration

### System Diagram
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    Bank     │     │   Gateway   │     │  Processor  │
│ Transactions│     │ Transactions│     │ Transactions│
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────────────────────────────────────────┐
│               Apache Kafka Topics               │
└───────────────────────┬─────────────────────────┘
                        │
       ┌────────────────┼────────────────┐
       │                │                │
       ▼                ▼                ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Flink Jobs  │  │   Kafka →   │  │ Spark Batch │
│ (Real-time  │  │    MinIO    │  │  Processing │
│  Analysis)  │  │             │  │             │
└─────────────┘  └─────────────┘  └──────┬──────┘
                                         │
                                         ▼
                                  ┌─────────────┐
                                  │   MinIO     │
                                  │  (Storage)  │
                                  └─────────────┘
                                         ▲
                                         │
                                  ┌─────────────┐
                                  │   Airflow   │
                                  │(Orchestration)
                                  └─────────────┘
```

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- 8GB+ RAM allocated to Docker

### Required Dependencies
The project requires the following JAR files (not included in the repository):
- `flink-connector-kafka-1.17.1.jar` - Flink connector for Kafka integration
- `flink-connector-kafka-4.0.0-2.0.jar` - Newer version of Flink connector 
- `kafka-clients-3.4.0.jar` - Kafka client library
- `kafka-clients-4.0.0.jar` - Newer version of Kafka client library

You can download these from:
- [Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [Kafka clients](https://kafka.apache.org/downloads)

Or by using Maven/Gradle to fetch them programmatically.

### Setup and Deployment

1. Clone the repository
```bash
git clone https://github.com/yourusername/fraudiq.git
cd fraudiq
```

2. Download required JAR files to project root
```bash
# Example using Maven to download the dependencies
mvn dependency:get -Dartifact=org.apache.flink:flink-connector-kafka:1.17.1
mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:3.4.0
```

3. Generate secure keys and set up environment variables
```bash
# Generate a secure Fernet key for Airflow
python utils/generate_fernet_key.py

# Create a .env file with your secure keys
cp .env.example .env
# Edit the .env file with your generated Fernet key and other credentials
```

Or
```bash
python utils/generate_fernet_key.py
```
and paste the generated key in docker_compose.yml
```yml
AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}
```

4. Start the services
```bash
docker-compose up -d
```

5. Access the Airflow UI
   - URL: http://localhost:8080
   - Username: admin
   - Password: airflow

6. Create S3 buckets in MinIO
```bash
python utils/upload_to_minio.py
```

7. Generate and upload sample data
```bash
# Generate data
python kafka/producer/generate_transactions.py --count 100

# Produce to Kafka
cat kafka/sample_data.json | docker exec -i fraudiq-kafka kafka-console-producer --broker-list localhost:9092 --topic bank_transactions
cat kafka/sample_data.json | docker exec -i fraudiq-kafka kafka-console-producer --broker-list localhost:9092 --topic gateway_transactions
cat kafka/sample_data.json | docker exec -i fraudiq-kafka kafka-console-producer --broker-list localhost:9092 --topic processor_transactions
```

8. Trigger Airflow DAGs
   - Open Airflow UI
   - Enable and trigger the `fraud_feature_extraction` DAG
   - Enable and trigger the `transaction_reconciliation` DAG

### Service Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | Workflow management |
| Kafka UI | http://localhost:9021 | Kafka cluster manager |
| Flink UI | http://localhost:8081 | Flink job dashboard |
| Spark UI | http://localhost:8090 | Spark master/worker status |
| MinIO Console | http://localhost:9001 | S3 storage management |

## Component Details

### Kafka
- Manages data streams from multiple sources
- Topics:
  - `bank_transactions`: Transactions from banking systems
  - `gateway_transactions`: Transactions from payment gateways
  - `processor_transactions`: Transactions from payment processors

### Flink
- Real-time processing and fraud detection
- Processes events from Kafka with low latency
- Implements complex event processing (CEP) patterns

### Spark
- Batch processing for feature extraction and reconciliation
- Main jobs:
  - `extract_features.py`: Analyzes transaction patterns and extracts fraud indicators
  - `reconcile.py`: Compares transactions across sources to identify discrepancies

### Airflow
- Orchestrates workflows for batch processing
- DAGs:
  - `fraud_feature_extraction`: Runs Spark jobs for feature extraction
  - `transaction_reconciliation`: Runs Spark jobs for transaction reconciliation

### MinIO
- S3-compatible object storage
- Buckets:
  - `raw`: Raw transaction data
  - `fraud-features`: Extracted fraud detection features
  - `reconciled`: Reconciliation results

## Troubleshooting

### Common Issues and Solutions

#### Airflow DAGs Not Showing Up
- **Issue**: DAGs are not visible in the Airflow UI
- **Solution**: 
  - Verify that DAG files are in the correct directory (`airflow/dags/`)
  - Check for syntax errors in DAG files
  - Run `airflow dags list` to see if Airflow can discover the DAGs

#### Spark Jobs Failing in Airflow
- **Issue**: Airflow shows error when submitting Spark jobs
- **Solution**:
  - Check the Spark connection configuration in Airflow
  - Ensure the connection URL format is `spark://spark-master:7077`
  - Verify Spark dependencies are available (Hadoop/S3 connectors)
  - Check Spark worker logs for detailed error messages

#### S3/MinIO Connection Issues
- **Issue**: Spark jobs can't access data in MinIO
- **Solution**:
  - Verify MinIO service is running
  - Check S3 endpoint configuration is correct (`http://minio:9000`)
  - Ensure access/secret keys are correct
  - Confirm the required buckets exist
  - Add AWS Hadoop JARs to Spark classpath for S3 connectivity

#### Kafka Topics Not Receiving Data
- **Issue**: No data appears in Kafka topics
- **Solution**:
  - Verify Kafka brokers are running
  - Check producer configurations
  - Use Kafka console consumer to test topic connectivity:
    ```
    docker exec -it fraudiq-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bank_transactions --from-beginning
    ```

#### Missing JAR Files
- **Issue**: Error about missing JAR files when starting services
- **Solution**:
  - Download the required JAR files as mentioned in the Prerequisites section
  - Place them in the correct locations as specified in `docker-compose.yml`
  - Restart the affected services

#### Memory Issues
- **Issue**: Services crashing with out-of-memory errors
- **Solution**:
  - Increase Docker memory allocation (minimum 8GB recommended)
  - Adjust memory settings in `docker-compose.yml`
  - Reduce parallelism in Spark/Flink configurations

#### Network Connectivity Problems
- **Issue**: Services can't communicate with each other
- **Solution**:
  - Ensure all services are on the same Docker network
  - Check that hostname resolution works between containers
  - Verify port mappings in `docker-compose.yml`

### Diagnostic Commands

```bash
# Check running containers
docker ps

# View container logs
docker logs fraudiq-airflow
docker logs fraudiq-kafka
docker logs fraudiq-spark-master

# Check Kafka topics
docker exec -it fraudiq-kafka kafka-topics --list --bootstrap-server localhost:9092

# Test MinIO connectivity
docker exec -it fraudiq-minio mc ls minio

# Verify Spark cluster status
docker exec -it fraudiq-spark-master spark-submit --version
```

## Advanced Configuration

### Scaling the System
- Increase Kafka partitions for higher throughput
- Add more Spark workers for increased processing capacity
- Configure Flink parallelism for better performance

### Security Configuration
- Enable authentication for all services
- Implement TLS/SSL for secure communication
- Configure proper access controls for MinIO buckets

## License

MIT 

## Contact

Ranjan Akarsh - [Instagram](https://instagram.com/ranjanakarsh)
