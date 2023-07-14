package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.spark.executor.Executor;
import org.apache.spark.scheduler.*;

public class ApacheSparkSingletons {

  public static final OpenTelemetry OPEN_TELEMETRY = GlobalOpenTelemetry.get();

  public static final String INSTRUMENTATION_NAME = "apache-spark";

  public static final PropertiesTextMapAccessor PROPERTIES_TEXT_MAP_ACCESSOR =
      new PropertiesTextMapAccessor();

  public static final Tracer TRACER =
      OPEN_TELEMETRY.getTracer("io.opentelemetry.apache-spark", "0.2.0");
  private static Instrumenter<TaskDescription, Object> TASK_RUNNER_INSTRUMENTER = null;

  public static final VirtualField<Stage, Context> STAGE_CONTEXT_VIRTUAL_FIELD =
      VirtualField.find(Stage.class, Context.class);

  public static final VirtualField<ActiveJob, Context> JOB_CONTEXT_VIRTUAL_FIELD =
      VirtualField.find(ActiveJob.class, Context.class);

  private static SpanNameExtractor<TaskDescription> TASK_SPAN_NAME_EXTRACTOR =
      taskDescription -> "spark-task";

  private static final AttributeKey<Long> SPARK_TASK_ID_ATTR_KEY =
      AttributeKey.longKey("spark.task_id");

  private static final AttributeKey<Long> SPARK_TASK_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.task_attempt_number");
  private static AttributesExtractor<TaskDescription, Object> TASK_ATTRIBUTES_EXTRACTOR =
      new AttributesExtractor<TaskDescription, Object>() {
        @Override
        public void onStart(
            AttributesBuilder attributes, Context parentContext, TaskDescription request) {
          attributes.put(SPARK_TASK_ID_ATTR_KEY, request.taskId());
          attributes.put(SPARK_TASK_ATTEMPT_NUMBER_ATTR_KEY, Long.valueOf(request.attemptNumber()));
        }

        @Override
        public void onEnd(
            AttributesBuilder attributes,
            Context context,
            TaskDescription request,
            @Nullable Object response,
            @Nullable Throwable error) {}
      };

  private static Method taskDescriptionAccessor = null;

  private static final Map<Integer, ActiveJob> JOB_REGISTRY = new HashMap<>();

  private static final Map<Integer, Stage> STAGE_REGISTRY = new HashMap<>();

  private static void initTaskInstrumenter() {

    TASK_RUNNER_INSTRUMENTER =
        Instrumenter.builder(OPEN_TELEMETRY, INSTRUMENTATION_NAME, TASK_SPAN_NAME_EXTRACTOR)
            .addAttributesExtractor(TASK_ATTRIBUTES_EXTRACTOR)
            .buildInstrumenter();
  }

  public static ActiveJob findJob(Integer jobId) {
    return JOB_REGISTRY.get(jobId);
  }

  public static Stage findStage(Integer stageId) {
    return STAGE_REGISTRY.get(stageId);
  }

  public static void setJobContext(ActiveJob job, Context context) {
    JOB_CONTEXT_VIRTUAL_FIELD.set(job, context);
  }

  public static Context getJobContext(ActiveJob job) {
    return JOB_CONTEXT_VIRTUAL_FIELD.get(job);
  }

  public static void setStageContext(Stage stage, Context context) {
    STAGE_CONTEXT_VIRTUAL_FIELD.set(stage, context);
  }

  public static Context getStageContext(Stage stage) {
    return STAGE_CONTEXT_VIRTUAL_FIELD.get(stage);
  }

  public static Instrumenter<TaskDescription, Object> taskRunnerInstrumenter() {
    if (TASK_RUNNER_INSTRUMENTER == null) {
      initTaskInstrumenter();
    }
    return TASK_RUNNER_INSTRUMENTER;
  }

  public static TaskDescription getTaskDescription(Executor.TaskRunner taskRunner)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    if (taskDescriptionAccessor == null) {
      taskDescriptionAccessor =
          Executor.TaskRunner.class.getMethod(
              "org$apache$spark$executor$Executor$TaskRunner$$taskDescription", new Class[0]);
    }

    return (TaskDescription) taskDescriptionAccessor.invoke(taskRunner);
  }

  private ApacheSparkSingletons() {}

  public static void registerJob(ActiveJob job) {
    JOB_REGISTRY.put(job.jobId(), job);
  }

  public static void registerStage(Stage stage) {
    STAGE_REGISTRY.put(stage.id(), stage);
  }

  public static void unregisterJob(ActiveJob job) {
    JOB_REGISTRY.remove(job.jobId());
  }

  public static void unregisterStage(Stage stage) {
    STAGE_REGISTRY.remove(stage.id());
  }
}
