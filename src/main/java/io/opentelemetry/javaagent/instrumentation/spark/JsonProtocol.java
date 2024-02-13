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
