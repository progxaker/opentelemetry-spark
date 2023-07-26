package io.opentelemetry.javaagent.instrumentation.spark.v3;

import static io.opentelemetry.javaagent.instrumentation.spark.v3.ApacheSparkSingletons.registerJob;
import static net.bytebuddy.asm.Advice.*;
import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Properties;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.JobListener;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.util.CallSite;

public class ActiveJobInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.ActiveJob");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {

    typeTransformer.applyAdviceToMethod(
        isConstructor()
            .and(takesArgument(0, Integer.TYPE))
            .and(takesArgument(1, named("org.apache.spark.scheduler.Stage")))
            .and(takesArgument(2, named("org.apache.spark.util.CallSite")))
            .and(takesArgument(3, named("org.apache.spark.scheduler.JobListener")))
            .and(takesArgument(4, java.util.Properties.class)),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @OnMethodExit()
    public static void exit(
        @This ActiveJob activeJob,
        @Argument(0) int jobId,
        @Argument(1) Stage finalStage,
        @Argument(2) CallSite callSite,
        @Argument(3) JobListener listener,
        @Argument(4) Properties properties) {

      registerJob(activeJob);
    }
  }
}
