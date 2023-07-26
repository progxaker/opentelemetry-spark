package io.opentelemetry.javaagent.instrumentation.spark.v3;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers;
import java.util.Arrays;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;

@AutoService(InstrumentationModule.class)
public class ApacheSparkInstrumentationModule extends InstrumentationModule {

  public ApacheSparkInstrumentationModule() {
    super("apache-spark", "apache-spark-3");
  }

  @Override
  public ElementMatcher.Junction<ClassLoader> classLoaderMatcher() {
    return AgentElementMatchers.hasClassesNamed(
        "org.apache.spark.scheduler.LiveListenerBus",
        "org.apache.spark.scheduler.Task",
        "org.apache.spark.executor.Executor$TaskRunner");
  }

  public List<TypeInstrumentation> typeInstrumentations() {
    return Arrays.asList(
        new LiveListenerBusInstrumentation(),
        new TaskRunnerInstrumentation(),
        new ActiveJobInstrumentation(),
        new StageInstrumentation(),
        new TaskInstrumentation());
  }

  @Override
  public List<String> getAdditionalHelperClassNames() {
    return Arrays.asList(
        "io.opentelemetry.javaagent.instrumentation.spark.v3.SparkEventListener",
        "io.opentelemetry.javaagent.instrumentation.spark.v3.ApacheSparkSingletons",
        "io.opentelemetry.javaagent.instrumentation.spark.v3.SparkAppResource",
        "io.opentelemetry.javaagent.instrumentation.spark.v3.SparkAppResourceProvider",
        "io.opentelemetry.javaagent.instrumentation.spark.v3.PropertiesTextMapAccessor");
  }
}
