/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023,2024 Key Tiong TAN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
