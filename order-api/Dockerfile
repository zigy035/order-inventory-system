# syntax=docker/dockerfile:1

# ---------- Build stage ----------
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY order-api/pom.xml order-api/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl order-api dependency:go-offline

COPY order-api/src order-api/src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl order-api package -DskipTests

# ---------- Runtime stage ----------
FROM eclipse-temurin:25-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
COPY --from=build /workspace/order-api/target/order-api-*.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
