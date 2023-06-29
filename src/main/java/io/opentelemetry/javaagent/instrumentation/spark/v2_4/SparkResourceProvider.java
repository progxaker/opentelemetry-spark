package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ResourceProvider;
import io.opentelemetry.sdk.resources.Resource;

/** {@link ResourceProvider} for automatically configuring {@link ResourceProvider}. */
@AutoService(ResourceProvider.class)
public class SparkResourceProvider implements ResourceProvider {
  @Override
  public Resource createResource(ConfigProperties config) {
    return SparkResource.get();
  }
}
