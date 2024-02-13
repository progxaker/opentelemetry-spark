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

import static io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.*;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.javaagent.bootstrap.Java8BytecodeBridge;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Properties;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.spark.executor.Executor;
import org.apache.spark.scheduler.TaskDescription;

public class TaskRunnerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return ElementMatchers.<TypeDescription>named("org.apache.spark.executor.Executor$TaskRunner");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {
    typeTransformer.applyAdviceToMethod(
        ElementMatchers.<MethodDescription>named("run")
            .and(ElementMatchers.<MethodDescription>isPublic()),
        this.getClass().getName() + "$TaskRunnerAdvice");
  }

  public static class TaskRunnerAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.This Executor.TaskRunner taskRunner,
        @Advice.Local("otelContext") Context context,
        @Advice.Local("otelScope") Scope scope)
        throws IllegalAccessException {

      TaskDescription taskDescription =
          io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.getTaskDescription(
              taskRunner);

      Properties localProperties = taskDescription.properties();
      Context rootContext = Java8BytecodeBridge.rootContext();

      Context parentContext =
          OPEN_TELEMETRY
              .getPropagators()
              .getTextMapPropagator()
              .extract(rootContext, localProperties, PROPERTIES_TEXT_MAP_ACCESSOR);

      if (!taskRunnerInstrumenter().shouldStart(parentContext, taskDescription)) {
        return;
      }

      context = taskRunnerInstrumenter().start(parentContext, taskDescription);
      scope = context.makeCurrent();
    }

    @Advice.OnMethodExit(suppress = Throwable.class, onThrowable = Throwable.class)
    public static void onExit(
        @Advice.This Executor.TaskRunner taskRunner,
        @Advice.Thrown Throwable exception,
        @Advice.Local("otelContext") Context context,
        @Advice.Local("otelScope") Scope scope)
        throws IllegalAccessException {

      TaskDescription taskDescription =
          io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.getTaskDescription(
              taskRunner);

      if (scope == null) {
        return;
      }
      scope.close();
      taskRunnerInstrumenter().end(context, taskDescription, null, exception);
    }
  }
}
