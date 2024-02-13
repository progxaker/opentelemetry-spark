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
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.resources.Resource;

public final class SparkResource {

  private static Resource INSTANCE = buildSingleton();

  private static Resource buildSingleton() {
    return new SparkResource().buildResource();
  }

  // Visible for testing
  SparkResource() {}

  private static String parseExecutorId() {
    String[] args = ProcessHandle.current().info().arguments().orElseGet(() -> new String[0]);

    String executorId = null;

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--executor-id")) {
        if (i < (args.length - 1)) {
          executorId = args[i + 1];
        }
        break;
      }
    }
    return executorId;
  }

  Resource buildResource() {

    String containerIdString = System.getenv("CONTAINER_ID");

    String executorId = parseExecutorId();

    if (containerIdString != null) {
      ContainerId containerId = ContainerId.fromString(containerIdString);
      ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
      ApplicationId applicationId = applicationAttemptId.getApplicationId();

      AttributesBuilder attributesBuilder = Attributes.builder();

      attributesBuilder.put(
          AttributeKey.stringKey("spark.application_id"), applicationId.toString());
      attributesBuilder.put(
          AttributeKey.stringKey("spark.application_attempt_id"), applicationAttemptId.toString());
      attributesBuilder.put(AttributeKey.stringKey("spark.container_id"), containerId.toString());

      if (executorId != null) {
        attributesBuilder.put(AttributeKey.stringKey("spark.executor_id"), executorId);
      }

      Attributes attrs = attributesBuilder.build();

      return Resource.create(attrs);
    } else {
      return Resource.empty();
    }
  }

  /** Returns resource with container information. */
  public static Resource get() {
    return INSTANCE;
  }
}
