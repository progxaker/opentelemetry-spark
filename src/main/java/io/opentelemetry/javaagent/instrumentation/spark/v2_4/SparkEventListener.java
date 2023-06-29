package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import java.util.concurrent.TimeUnit;
import org.apache.spark.scheduler.*;
import scala.Some;
import scala.collection.JavaConversions;

public class SparkEventListener {
  private static Span applicationSpan;

  private static Context applicationContext;

  public static void onApplicationStart(SparkListenerApplicationStart event) {

    String appId = event.appId().get();
    String attemptId = null;
    if (event.appAttemptId() instanceof Some) {
      attemptId = event.appAttemptId().get();
    }

    applicationSpan =
        ApacheSparkSingletons.TRACER
            .spanBuilder(
                String.format("Spark Application %s", attemptId != null ? attemptId : appId))
            .setParent(Context.current())
            .setStartTimestamp(event.time(), TimeUnit.MILLISECONDS)
            .startSpan();

    Attributes attrs =
        Attributes.of(
            AttributeKey.stringKey("event_type"),
            "application_start",
            AttributeKey.stringKey("application_name"),
            event.appName(),
            AttributeKey.stringKey("application_id"),
            appId,
            AttributeKey.stringKey("spark_user"),
            event.sparkUser());

    if (attemptId != null) {
      attrs =
          attrs.toBuilder()
              .put(AttributeKey.stringKey("application_attempt_id"), attemptId)
              .build();
    }

    applicationSpan.addEvent(
        "Spark Event : Application Start", attrs, event.time(), TimeUnit.MILLISECONDS);

    applicationContext = applicationSpan.storeInContext(Context.current());
  }

  public static void onApplicationEnd(SparkListenerApplicationEnd event) {

    Span s = Span.fromContext(applicationContext);
    Attributes attrs = Attributes.of(AttributeKey.stringKey("event_type"), "application_end");
    s.addEvent("Spark Event : Application End", attrs, event.time(), TimeUnit.MILLISECONDS);
    s.end(event.time(), TimeUnit.MILLISECONDS);
    applicationContext = null;
  }

  public static void onJobStart(SparkListenerJobStart event) {

    Integer jobId = event.jobId();
    ActiveJob job = ApacheSparkSingletons.findJob(jobId);
    Context parentContext = Context.current();

    Span jobSpan =
        ApacheSparkSingletons.TRACER
            .spanBuilder(String.format("Job %s", job.jobId()))
            .setParent(parentContext)
            .setStartTimestamp(event.time(), TimeUnit.MILLISECONDS)
            .startSpan();
    Context jobContext = jobSpan.storeInContext(parentContext);

    ApacheSparkSingletons.setJobContext(job, jobContext);
  }

  public static void onJobEnd(SparkListenerJobEnd event) {

    Integer jobId = event.jobId();
    ActiveJob job = ApacheSparkSingletons.findJob(jobId);

    Context jobContext = ApacheSparkSingletons.getJobContext(job);

    Span span = Span.fromContext(jobContext);

    JobResult jobResult = event.jobResult();

    if (jobResult instanceof JobSucceeded$) {
      JobSucceeded$ okResult = (JobSucceeded$) jobResult;
      span.setStatus(StatusCode.OK);
    } else if (jobResult instanceof JobFailed) {
      JobFailed errorResult = (JobFailed) jobResult;
      span.recordException(errorResult.exception());
      span.setStatus(StatusCode.ERROR);
    }

    span.end(event.time(), TimeUnit.MILLISECONDS);
    ApacheSparkSingletons.unregisterJob(job);
  }

  public static void onStageSubmitted(SparkListenerStageSubmitted event) {

    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();

    Stage stage = ApacheSparkSingletons.findStage(stageId);

    Integer jobId = stage.firstJobId();
    ActiveJob firstJob = ApacheSparkSingletons.findJob(jobId);
    Context firstJobContext = ApacheSparkSingletons.getJobContext(firstJob);

    Integer attemptId = stageInfo.attemptId();

    SpanBuilder builder =
        ApacheSparkSingletons.TRACER
            .spanBuilder(String.format("Stage %s", stage.id()))
            .setParent(firstJobContext)
            .setAttribute("stage_id", stageId)
            .setAttribute("stage_atteampt_id", attemptId)
            .setStartTimestamp((Long) stageInfo.submissionTime().get(), TimeUnit.MILLISECONDS);

    for (Object id : JavaConversions.asJavaCollection(stage.jobIds())) {
      Integer jid = (Integer) id;
      ActiveJob job = ApacheSparkSingletons.findJob(jid);
      Context jcontext = ApacheSparkSingletons.getJobContext(job);
      Span s = Span.fromContext(jcontext);
      builder.addLink(s.getSpanContext());
    }
    Span stageSpan = builder.startSpan();
    Context stageContext = stageSpan.storeInContext(firstJobContext);
    ApacheSparkSingletons.setStageContext(stage, stageContext);
  }

  public static void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();
    stageInfo.completionTime();
    Stage stage = ApacheSparkSingletons.findStage(stageId);
    Context stageContext = ApacheSparkSingletons.getStageContext(stage);
    if (stageContext != null) {
      Span span = Span.fromContext(stageContext);
      if (stageInfo.failureReason() instanceof Some) {
        span.setStatus(StatusCode.ERROR, stageInfo.failureReason().get());
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end((Long) stageInfo.completionTime().get(), TimeUnit.MILLISECONDS);
    }
    ApacheSparkSingletons.unregisterStage(stage);
  }

  public static void onTaskStart(SparkListenerTaskStart event) {}

  public static void onTaskEnd(SparkListenerTaskEnd event) {}

  public static void onOtherEvent(SparkListenerEvent event) {}
}
