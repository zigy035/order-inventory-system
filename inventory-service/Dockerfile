# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY order-api/pom.xml order-api/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl inventory-service dependency:go-offline

COPY inventory-service/src inventory-service/src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl inventory-service package -DskipTests

# ---------- Runtime stage ----------
FROM eclipse-temurin:25-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
COPY --from=build /workspace/inventory-service/target/inventory-service-*.jar app.jar
USER app
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
