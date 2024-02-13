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

import static io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.OPEN_TELEMETRY;
import static io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.PROPERTIES_TEXT_MAP_ACCESSOR;
import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Properties;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.scheduler.Stage;

public class TaskInstrumentation_v2_4 implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.Task");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {

    typeTransformer.applyAdviceToMethod(
        isConstructor()
            .and(takesArgument(0, Integer.TYPE))
            .and(takesArgument(1, Integer.TYPE))
            .and(takesArgument(2, Integer.TYPE))
            .and(takesArgument(3, Properties.class))
            .and(takesArgument(4, byte[].class))
            .and(takesArgument(5, named("scala.Option")))
            .and(takesArgument(6, named("scala.Option")))
            .and(takesArgument(7, named("scala.Option")))
            .and(takesArgument(8, Boolean.TYPE)),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void enter(
        int stageId,
        int stageAttemptId,
        int partition,
        Properties localProperties,
        byte[] serializedTaskMetrics,
        scala.Option jobId,
        scala.Option appId,
        scala.Option appAttemptId,
        boolean isBarrier) {

      Stage stage = ApacheSparkSingletons.findStage(stageId);
      Context stageContext = ApacheSparkSingletons.getStageContext(stage);

      OPEN_TELEMETRY
          .getPropagators()
          .getTextMapPropagator()
          .inject(stageContext, localProperties, PROPERTIES_TEXT_MAP_ACCESSOR);
    }
  }
}
