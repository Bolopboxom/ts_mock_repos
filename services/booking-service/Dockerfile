FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app
COPY target/booking-service-0.1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
