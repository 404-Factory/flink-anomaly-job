FROM gradle:8.5-jdk17 AS build
WORKDIR /app
COPY build.gradle settings.gradle lombok.config ./
COPY src ./src
RUN gradle jar --no-daemon

FROM flink:1.18.1-java17
WORKDIR /opt/flink/usrlib

# Enable the s3a:// filesystem plugin (Parquet branch writes to S3). Hadoop classes
# Parquet itself needs are bundled in the job jar.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Flink Kubernetes Operator (application mode) runs this jar via jarURI.
COPY --from=build /app/build/libs/*.jar flink-unified-job.jar
