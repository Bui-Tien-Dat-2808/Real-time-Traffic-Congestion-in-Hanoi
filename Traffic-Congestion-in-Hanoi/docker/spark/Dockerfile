FROM apache/spark:3.5.0

USER root
RUN mkdir -p /opt/spark/jars-extra && \
    curl -fsSL -o /opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars-extra/kafka-clients-3.4.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars-extra/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/jars-extra/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -fsSL -o /opt/spark/jars-extra/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    cp /opt/spark/jars-extra/*.jar /opt/spark/jars/

USER spark
