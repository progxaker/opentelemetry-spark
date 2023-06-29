package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import static io.opentelemetry.javaagent.instrumentation.spark.v2_4.ApacheSparkSingletons.*;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.javaagent.bootstrap.Java8BytecodeBridge;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.lang.reflect.InvocationTargetException;
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
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

      TaskDescription taskDescription = getTaskDescription(taskRunner);

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
        throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {

      TaskDescription taskDescription = getTaskDescription(taskRunner);

      if (scope == null) {
        return;
      }
      scope.close();
      taskRunnerInstrumenter().end(context, taskDescription, null, exception);
    }
  }
}
