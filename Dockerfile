FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY src ./src
RUN gradle jar --no-daemon

FROM flink:1.18.1-java17
WORKDIR /opt/flink/usrlib
COPY --from=build /app/build/libs/*.jar flink-anomaly-job.jar
ENTRYPOINT ["java", \
  "--add-opens", "java.base/java.util=ALL-UNNAMED", \
  "--add-opens", "java.base/java.lang=ALL-UNNAMED", \
  "--add-opens", "java.base/java.time=ALL-UNNAMED", \
  "-jar", "/opt/flink/usrlib/flink-anomaly-job.jar"]
