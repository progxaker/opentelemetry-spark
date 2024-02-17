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
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.scheduler.SparkListenerEvent;

public class EventTimeAccessor {

  private static Map<Class<? extends SparkListenerEvent>, Method> methodMap = new HashMap<>();

  public static Long getEventTime(SparkListenerEvent event) {
    Class<? extends SparkListenerEvent> clazz = event.getClass();

    if (!methodMap.containsKey(clazz)) {
      Method eventMethod = null;
      try {
        eventMethod = clazz.getMethod("event");
      } catch (NoSuchMethodException e) {
        // ignore;
      }
      methodMap.put(clazz, eventMethod);
    }

    Method m = methodMap.get(clazz);
    if (m != null) {
      Long eventTime;
      try {
        eventTime = (Long) m.invoke(event);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      }
      return eventTime;
    } else {
      return null;
    }
  }
}
