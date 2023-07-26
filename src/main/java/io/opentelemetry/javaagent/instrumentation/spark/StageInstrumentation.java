package io.opentelemetry.javaagent.instrumentation.spark;

import static net.bytebuddy.asm.Advice.Argument;
import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.util.CallSite;

public class StageInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.Stage");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {

    typeTransformer.applyAdviceToMethod(
        isConstructor()
            .and(takesArgument(0, Integer.TYPE))
            .and(takesArgument(1, named("org.apache.spark.rdd.RDD")))
            .and(takesArgument(2, Integer.TYPE))
            .and(takesArgument(3, named("scala.collection.immutable.List")))
            .and(takesArgument(4, Integer.TYPE))
            .and(takesArgument(5, named("org.apache.spark.util.CallSite"))),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @Advice.OnMethodExit()
    public static void exit(
        @Advice.This Stage stage,
        @Argument(0) int stageId,
        @Argument(1) RDD rdd,
        @Argument(2) int numTasks,
        @Argument(3) scala.collection.immutable.List parents,
        @Argument(4) int firstJobId,
        @Argument(5) CallSite callSite) {
      ApacheSparkSingletons.registerStage(stage);
    }
  }
}
