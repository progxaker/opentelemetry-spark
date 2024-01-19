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

public class TaskInstrumentation_v3_4 implements TypeInstrumentation {
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
            .and(takesArgument(3, Integer.TYPE))
            .and(takesArgument(4, Properties.class))
            .and(takesArgument(5, byte[].class))
            .and(takesArgument(6, named("scala.Option")))
            .and(takesArgument(7, named("scala.Option")))
            .and(takesArgument(8, named("scala.Option")))
            .and(takesArgument(9, Boolean.TYPE)),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void enter(
        int stageId,
        int stageAttemptId,
        int partitionId,
        int numPartitions,
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
