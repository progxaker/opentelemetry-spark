package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

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

      if (event instanceof SparkListenerApplicationStart) {
        SparkEventListener.onApplicationStart((SparkListenerApplicationStart) event);
      } else if (event instanceof SparkListenerApplicationEnd) {
        SparkEventListener.onApplicationEnd((SparkListenerApplicationEnd) event);
      } else if (event instanceof SparkListenerJobStart) {
        SparkEventListener.onJobStart((SparkListenerJobStart) event);
      } else if (event instanceof SparkListenerJobEnd) {
        SparkEventListener.onJobEnd((SparkListenerJobEnd) event);
      } else if (event instanceof SparkListenerStageSubmitted) {
        SparkEventListener.onStageSubmitted((SparkListenerStageSubmitted) event);
      } else if (event instanceof SparkListenerStageCompleted) {
        SparkEventListener.onStageCompleted((SparkListenerStageCompleted) event);
      } else if (event instanceof SparkListenerTaskStart) {
        SparkEventListener.onTaskStart((SparkListenerTaskStart) event);
      } else if (event instanceof SparkListenerTaskEnd) {
        SparkEventListener.onTaskEnd((SparkListenerTaskEnd) event);
      } else {
        SparkEventListener.onOtherEvent(event);
      }
    }
  }
}
