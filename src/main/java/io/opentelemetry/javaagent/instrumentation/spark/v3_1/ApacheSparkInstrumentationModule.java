package io.opentelemetry.javaagent.instrumentation.spark.v3_1;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers;
import io.opentelemetry.javaagent.instrumentation.spark.ActiveJobInstrumentation;
import io.opentelemetry.javaagent.instrumentation.spark.StageInstrumentation;
import io.opentelemetry.javaagent.instrumentation.spark.TaskRunnerInstrumentation;
import java.util.Arrays;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;

@AutoService(InstrumentationModule.class)
public class ApacheSparkInstrumentationModule extends InstrumentationModule {

  public ApacheSparkInstrumentationModule() {
    super("apache-spark", "apache-spark-3.1");
  }

  @Override
  public ElementMatcher.Junction<ClassLoader> classLoaderMatcher() {
    return AgentElementMatchers.hasClassesNamed(
        "org.apache.spark.scheduler.LiveListenerBus",
        "org.apache.spark.scheduler.Task",
        "org.apache.spark.executor.Executor$TaskRunner",
        "org.apache.spark.scheduler.SparkListenerResourceProfileAdded");
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
        "io.opentelemetry.javaagent.instrumentation.spark.v3_1.SparkEventListener",
        "io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons",
        "io.opentelemetry.javaagent.instrumentation.spark.PropertiesTextMapAccessor");
  }
}
