FROM flink:1.18.1-java17
WORKDIR /opt/flink/usrlib

# Enable the s3a:// filesystem plugin (Parquet branch writes to S3). This plugin
# only serves the S3 filesystem; the raw org.apache.hadoop.* classes the Parquet
# sink needs are bundled in the job jar (see build.gradle).
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/

# Flink Kubernetes Operator (application mode) runs this jar via jarURI.
# Pre-built jar from workflow (gradle build runs once, jar reused for all platforms)
COPY build/libs/*.jar flink-anomaly-job.jar
