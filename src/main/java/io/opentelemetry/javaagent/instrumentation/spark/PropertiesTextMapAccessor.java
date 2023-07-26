package io.opentelemetry.javaagent.instrumentation.spark;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

public class PropertiesTextMapAccessor
    implements TextMapSetter<Properties>, TextMapGetter<Properties> {
  @Override
  public Iterable<String> keys(Properties properties) {
    Set<String> stringKeys = properties.stringPropertyNames();
    return new LinkedList<>(stringKeys);
  }

  @Nullable
  @Override
  public String get(@Nullable Properties properties, String key) {
    return properties.getProperty(key);
  }

  @Override
  public void set(@Nullable Properties properties, String key, String value) {
    properties.setProperty(key, value);
  }
}
