FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/reporting-gateway-1.0-SNAPSHOT.jar gateway.jar
COPY target/dependency/ lib/
USER 1001
ENTRYPOINT ["java", "-cp", "gateway.jar:lib/*", "com.helesto.journal.ProjectionGateway"]
