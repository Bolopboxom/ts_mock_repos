Mô tả cấu trúc thư mục cho `booking-service` (DDD + microservice-ready)

services/booking-service/
├─ pom.xml                         # Maven project (Spring Boot 3, Spring Kafka, MyBatis, Resilience4j, Actuator)
├─ Dockerfile
├─ README.md
├─ STRUCTURE.md                    # (this file) mô tả cấu trúc
├─ src/
│  ├─ main/
│  │  ├─ java/com/airline/booking/
│  │  │  ├─ BookingApplication.java
│  │  │  ├─ config/                # cấu hình: Kafka, DataSource, Web, CorrelationId, Resilience
│  │  │  │  ├─ KafkaConfig.java
│  │  │  │  ├─ DataSourceConfig.java
│  │  │  │  └─ WebConfig.java
│  │  │  ├─ domain/                # core domain layer (entities, value objects, domain services)
│  │  │  │  ├─ model/
│  │  │  │  │  └─ Booking.java
│  │  │  │  ├─ repository/         # repository interfaces (aggregate gateways)
│  │  │  │  │  └─ BookingRepository.java
│  │  │  ├─ application/           # use-cases / application services / DTOs
│  │  │  │  ├─ dto/
│  │  │  │  │  └─ BookingDto.java
│  │  │  │  └─ usecase/
│  │  │  │     └─ CreateBookingUseCase.java
│  │  │  ├─ infrastructure/        # adapters, persistence (MyBatis), kafka producer/consumer
│  │  │  │  ├─ persistence/
│  │  │  │  │  ├─ mapper/           # MyBatis mappers (.xml)
│  │  │  │  │  └─ BookingMapper.java
│  │  │  │  ├─ kafka/
│  │  │  │  │  ├─ producer/
│  │  │  │  │  │  └─ BookingEventProducer.java
│  │  │  │  │  └─ consumer/
│  │  │  │  └─ oracle/
│  │  │  │     └─ OracleBookingRepositoryImpl.java
│  │  │  └─ interfaces/            # external interfaces (REST controllers, DTOs)
│  │  │     ├─ rest/
│  │  │     │  └─ BookingController.java
│  │  │     └─ dto/                # HTTP request/response DTOs
│  │  └─ resources/
│  │     ├─ application.yml
│  │     ├─ application-dev.yml
│  │     ├─ logback-spring.xml
│  │     └─ db/                # database-related resources (migrations not included for now)
│  └─ test/
│     ├─ java/...                  # unit & integration tests (including Testcontainers)
│     └─ resources/
├─ scripts/
│  └─ run-local.ps1                # powershell script cho dev local
└─ docs/
   └─ SEQUENCE_BOOKING_CREATE.md

Ghi chú ngắn
- `pom.xml` nên khai báo: spring-boot-starter-web, spring-boot-starter-actuator, spring-kafka, mybatis-spring-boot-starter, resilience4j-spring-boot2, junit/testcontainers, lombok (tùy chọn).
- Event contract: publish `booking.created.v1` với envelope chứa correlationId, traceId, payload.
- Idempotency: consumer xử lý idempotent bằng eventId / dedup table.
- DB: cấu hình kết nối đến Oracle schema `BOOKING_SCHEMA` (datasource url, username, password).

Mục tiếp theo tôi có thể: tạo `pom.xml`, `BookingApplication.java`, controller + producer skeleton, và Dockerfile. Xác nhận nếu muốn tôi bắt đầu tạo file-code này.
