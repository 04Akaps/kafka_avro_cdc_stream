# Kafka Order Processing System

실시간 주문 처리를 위한 Kafka 기반 이벤트 스트리밍 시스템

## 기술 스택

- **Kotlin** + **Spring Boot** 3.2.0
- **Apache Kafka** + **Kafka Streams**
- **PostgreSQL** (CDC 소스)
- **Debezium** (Change Data Capture)
- **Apache Avro** (스키마 레지스트리)

## 시작하기

### 필요 환경
- Java 17
- Docker & Docker Compose

### 실행 방법

1. 인프라 시작
```bash
docker-compose up -d
```

2. Kafka Connect 설정
```bash
./kafka-connect-setup.sh
```

3. 애플리케이션 시작
```bash
./start.sh
```

## 서비스 포트

- 애플리케이션: `http://localhost:8080`
- Kafka UI: `http://localhost:8081`
- Kafka Connect: `http://localhost:8083`
- PostgreSQL: `localhost:5432`

## 주요 기능

- 실시간 주문 이벤트 처리
- CDC를 통한 데이터베이스 변경 감지
- Avro 스키마를 통한 이벤트 직렬화
- Kafka Streams를 통한 이벤트 스트리밍
- 주문 통계 및 분석