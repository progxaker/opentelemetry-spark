package io.opentelemetry.javaagent.instrumentation.spark;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import java.util.concurrent.TimeUnit;
import org.apache.spark.scheduler.SparkListenerEvent;

public class SparkEventLogger {

  private static final Logger SPARK_EVENT_LOGGER =
      ApacheSparkSingletons.LOGGER_PROVIDER.get(SparkEventLogger.class.getName());

  private static final AttributeKey<String> EVENT_NAME_ATTR_KEY =
      AttributeKey.stringKey("event.name");

  private static final AttributeKey<String> EVENT_DOMAIN_ATTR_KEY =
      AttributeKey.stringKey("event.domain");

  private static final String SPARK_EVENT_DOMAIN = "spark";

  public static void emitSparkEvent(SparkListenerEvent event) {
    emitSparkEvent(event, System.currentTimeMillis());
  }

  public static void emitSparkEvent(SparkListenerEvent event, Long timestamp) {

    String eventJsonString = JsonProtocol.sparkEventToJsonString(event);

    String eventName = event.getClass().getSimpleName();

    if (eventJsonString != null) {
      SPARK_EVENT_LOGGER
          .logRecordBuilder()
          .setTimestamp(timestamp, TimeUnit.MILLISECONDS)
          .setSeverity(Severity.INFO)
          .setAttribute(EVENT_NAME_ATTR_KEY, eventName)
          .setAttribute(EVENT_DOMAIN_ATTR_KEY, SPARK_EVENT_DOMAIN)
          .setBody(eventJsonString)
          .emit();
    }
  }
}
