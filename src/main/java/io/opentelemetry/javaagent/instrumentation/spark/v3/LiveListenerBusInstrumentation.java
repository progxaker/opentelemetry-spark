package io.opentelemetry.javaagent.instrumentation.spark.v3;

import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.scheduler.*;

public class LiveListenerBusInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.LiveListenerBus");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {
    typeTransformer.applyAdviceToMethod(
        isPublic()
            .and(
                named("post")
                    .and(takesArgument(0, named("org.apache.spark.scheduler.SparkListenerEvent")))),
        this.getClass().getName() + "$Interceptor");
  }

  @SuppressWarnings("unused")
  public static class Interceptor {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
        @Advice.This LiveListenerBus bus, @Advice.Argument(value = 0) SparkListenerEvent event) {
      SparkEventListener.handleSparkListenerEvent(event);
    }
  }
}
