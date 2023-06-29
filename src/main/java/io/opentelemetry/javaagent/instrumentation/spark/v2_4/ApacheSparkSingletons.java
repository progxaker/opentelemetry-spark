package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.executor.Executor;
import org.apache.spark.scheduler.*;

public class ApacheSparkSingletons {

  public static final Map<Long, Context> CONTEXT_BY_TASK_ID = new Hashtable<>();

  public static final Map<Long, TaskDescription> TASK_DESCRIPTION_BY_TASK_ID = new Hashtable<>();

  public static final OpenTelemetry OPEN_TELEMETRY = GlobalOpenTelemetry.get();

  public static final String INSTRUMENTATION_NAME = "apache-spark";

  public static final PropertiesTextMapAccessor PROPERTIES_TEXT_MAP_ACCESSOR =
      new PropertiesTextMapAccessor();

  public static final Tracer TRACER = OPEN_TELEMETRY.getTracer("apache-spark", "0.1.0");
  private static Instrumenter<TaskDescription, Object> TASK_RUNNER_INSTRUMENTER = null;

  public static final VirtualField<Stage, Context> STAGE_CONTEXT_VIRTUAL_FIELD =
      VirtualField.find(Stage.class, Context.class);

  public static final VirtualField<ActiveJob, Context> JOB_CONTEXT_VIRTUAL_FIELD =
      VirtualField.find(ActiveJob.class, Context.class);

  private static SpanNameExtractor<TaskDescription> spanNameExtractor =
      new SpanNameExtractor<TaskDescription>() {
        @Override
        public String extract(TaskDescription taskDescription) {
          return "task-" + taskDescription.taskId();
        }
      };

  private static Method taskDescriptionAccessor = null;

  private static SparkContext SPARK_CONTEXT = null;

  private static DAGScheduler DAG_SCHEDULER = null;

  private static Map<Integer, ActiveJob> JOB_REGISTRY = new HashMap<>();

  private static Map<Integer, Stage> STAGE_REGISTRY = new HashMap<>();

  private static void init() {

    TASK_RUNNER_INSTRUMENTER =
        Instrumenter.<TaskDescription, Object>builder(
                GlobalOpenTelemetry.get(), INSTRUMENTATION_NAME, spanNameExtractor)
            .buildInstrumenter();
  }

  public static SparkContext sparkContext() {
    if (SPARK_CONTEXT == null) {
      SparkContext ctx = SparkContext$.MODULE$.getActive().get();
      if (ctx != null) {
        SPARK_CONTEXT = ctx;
      }
    }
    return SPARK_CONTEXT;
  }

  public static DAGScheduler dagScheduler() {
    if (DAG_SCHEDULER == null) {
      SparkContext ctx = sparkContext();
      if (ctx != null) {
        DAG_SCHEDULER = ctx.dagScheduler();
      }
    }
    return DAG_SCHEDULER;
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
      init();
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
