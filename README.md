# OpenTelemetry Spark Instrumentation Extension

## Introduction

This is an OpenTelemetry extension to instrument [Apache Spark](https://spark.apache.org/) application. This is not be
confused with the existing OpenTelemetry Spark web framework auto instrumentation.

## Usage

1. Copy `opentelemetry-javaagent.jar` and this extension `opentelemetry-spark-all-<version>.jar` to all YARN node
manager hosts.
2. Submit spark application with the OpenTelemetry properties with Spark configurations `spark.driver.extraJavaOptions`
   and `spark.executor.extraJavaOptions`.

For example,

```shell
/path/to/spark-submit  \
 --class org.apache.spark.examples.SparkPi \
 --master yarn \
 --deploy-mode cluster \
 --executor-memory 2g \
 --executor-cores 1 \
 --conf spark.executor.instances=2 \
 --conf "spark.driver.extraJavaOptions=-javaagent:/path/to/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/path/to/opentelemetry-spark-0.1.0-all.jar -Dotel.traces.exporter=otlp -Dotel.metrics.exporter=otlp -Dotel.logs.exporter=otlp -Dotel.java.global-autoconfigure.enabled=true" \
 --conf "spark.executor.extraJavaOptions=-javaagent:/path/to/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/path/to/opentelemetry-spark-0.1.0-all.jar -Dotel.traces.exporter=otlp -Dotel.metrics.exporter=otlp -Dotel.logs.exporter=otlp -Dotel.java.global-autoconfigure.enabled=true" \
 /path/to/spark-examples_2.11-2.4.8.jar
```

## Build Extension
To build extension project, run `./gradlew build`. The resulting jar file in `build/libs`.
```