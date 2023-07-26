package io.opentelemetry.javaagent.instrumentation.spark;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.resources.Resource;

public final class SparkResource {

  private static Resource INSTANCE = buildSingleton();

  private static Resource buildSingleton() {
    return new SparkResource().buildResource();
  }

  // Visible for testing
  SparkResource() {}

  // Visible for testing
  Resource buildResource() {

    String containerIdString = System.getenv("CONTAINER_ID");

    if (containerIdString != null) {
      ContainerId containerId = ContainerId.fromString(containerIdString);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
      ApplicationId applicationId = applicationAttemptId.getApplicationId();

      return Resource.create(
          Attributes.of(
              AttributeKey.stringKey("spark.application_id"),
              applicationId.toString(),
              AttributeKey.stringKey("spark.application_attempt_id"),
              applicationAttemptId.toString(),
              AttributeKey.stringKey("spark.container_id"),
              containerId.toString()));
    } else {
      return Resource.empty();
    }
  }

  /** Returns resource with container information. */
  public static Resource get() {
    return INSTANCE;
  }
}
