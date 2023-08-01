package io.opentelemetry.javaagent.instrumentation.spark;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.json4s.JsonAST;
import org.json4s.jackson.JsonMethods$;

public class JsonProtocol {

  private static Method SPARK_EVENT_TO_JSON_METHOD;

  private static Method SPARK_EVENT_TO_JSON_STRING_METHOD;

  static {
    try {
      Class<?> jsonProtocolClass = Class.forName("org.apache.spark.util.JsonProtocol");
      try {
        SPARK_EVENT_TO_JSON_METHOD =
            jsonProtocolClass.getMethod("sparkEventToJson", SparkListenerEvent.class);
      } catch (NoSuchMethodException e) {
        // ignore
      }

      try {
        SPARK_EVENT_TO_JSON_STRING_METHOD =
            jsonProtocolClass.getMethod("sparkEventToJsonString", SparkListenerEvent.class);
      } catch (NoSuchMethodException e) {
        // ignore
      }

    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static String sparkEventToJsonString(SparkListenerEvent event) {
    if (SPARK_EVENT_TO_JSON_STRING_METHOD != null) {
      try {
        return (String) SPARK_EVENT_TO_JSON_STRING_METHOD.invoke(null, event);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    if (SPARK_EVENT_TO_JSON_METHOD != null) {
      try {
        JsonAST.JValue jValue = (JsonAST.JValue) SPARK_EVENT_TO_JSON_METHOD.invoke(null, event);
        return JsonMethods$.MODULE$.compact(jValue);
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }
}
