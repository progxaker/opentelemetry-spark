package io.opentelemetry.javaagent.instrumentation.spark.v3;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.scheduler.*;
import org.apache.spark.util.JsonProtocol;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class SparkEventListener {
  private static Span applicationSpan;

  private static Context applicationContext;

  private static final AttributeKey<String> EVENT_NAME_ATTR_KEY =
      AttributeKey.stringKey("event.name");

  private static final AttributeKey<String> EVENT_DOMAIN_ATTR_KEY =
      AttributeKey.stringKey("event.domain");

  private static final AttributeKey<String> SPARK_EVENT_ATTR_KEY =
      AttributeKey.stringKey("spark.event");

  private static final AttributeKey<String> SPARK_APPLICATION_NAME_ATTR_KEY =
      AttributeKey.stringKey("spark.application_name");

  private static final AttributeKey<Long> SPARK_JOB_ID_ATTR_KEY =
      AttributeKey.longKey("spark.job_id");

  private static final AttributeKey<Long> SPARK_STAGE_ID_ATTR_KEY =
      AttributeKey.longKey("spark.stage_id");

  private static final AttributeKey<Long> SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.stage_attempt_number");

  private static final String SPARK_EVENT_DOMAIN = "spark";

  private static final List<SparkListenerEvent> PENDING_EVENTS = new LinkedList<>();

  private static void initApplicationContext(String applicationName) {
    // spark.app.name
    applicationSpan =
        ApacheSparkSingletons.TRACER
            .spanBuilder("spark_application")
            .setParent(Context.current())
            .setAttribute(SPARK_APPLICATION_NAME_ATTR_KEY, applicationName)
            .startSpan();

    applicationContext = applicationSpan.storeInContext(Context.current());
  }

  private static void emitSparkEvent(SparkListenerEvent event) {
    emitSparkEvent(event, null);
  }

  private static void emitSparkEvent(SparkListenerEvent event, Long time) {

    Span s = Span.fromContext(applicationContext);

    // JsonAST.JValue jvalue = JsonProtocol.sparkEventToJson(event);
    String eventJson = JsonProtocol.sparkEventToJsonString(event);

    String eventName = event.getClass().getSimpleName();
    Attributes attrs =
        Attributes.of(EVENT_NAME_ATTR_KEY, eventName, EVENT_DOMAIN_ATTR_KEY, SPARK_EVENT_DOMAIN);

    if (time != null) {
      s.addEvent(eventJson, attrs, time, TimeUnit.MILLISECONDS);
    } else {
      s.addEvent(eventJson, attrs);
    }
  }

  public static void onApplicationStart(SparkListenerApplicationStart event) {
    emitSparkEvent(event, event.time());
  }

  public static void onApplicationEnd(SparkListenerApplicationEnd event) {
    emitSparkEvent(event, event.time());
    applicationSpan.end(event.time(), TimeUnit.MILLISECONDS);
    applicationContext = null;
  }

  public static void onJobStart(SparkListenerJobStart event) {

    Integer jobId = event.jobId();
    ActiveJob job = ApacheSparkSingletons.findJob(jobId);
    Context parentContext = Context.current();

    Span jobSpan =
        ApacheSparkSingletons.TRACER
            .spanBuilder("spark_job")
            .setAttribute(SPARK_JOB_ID_ATTR_KEY, Long.valueOf(jobId))
            .setParent(parentContext)
            .setStartTimestamp(event.time(), TimeUnit.MILLISECONDS)
            .addLink(applicationSpan.getSpanContext())
            .startSpan();
    Context jobContext = jobSpan.storeInContext(parentContext);

    ApacheSparkSingletons.setJobContext(job, jobContext);

    emitSparkEvent(event, event.time());
  }

  public static void onJobEnd(SparkListenerJobEnd event) {

    Integer jobId = event.jobId();
    ActiveJob job = ApacheSparkSingletons.findJob(jobId);

    Context jobContext = ApacheSparkSingletons.getJobContext(job);

    Span jobSpan = Span.fromContext(jobContext);

    JobResult jobResult = event.jobResult();

    if (jobResult instanceof JobSucceeded$) {
      jobSpan.setStatus(StatusCode.OK);
    } else if (jobResult instanceof JobFailed) {
      JobFailed errorResult = (JobFailed) jobResult;
      jobSpan.recordException(errorResult.exception());
      jobSpan.setStatus(StatusCode.ERROR);
    }

    jobSpan.end(event.time(), TimeUnit.MILLISECONDS);
    ApacheSparkSingletons.unregisterJob(job);

    emitSparkEvent(event, event.time());
  }

  public static void onStageSubmitted(SparkListenerStageSubmitted event) {

    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();

    Stage stage = ApacheSparkSingletons.findStage(stageId);

    Integer jobId = stage.firstJobId();
    ActiveJob firstJob = ApacheSparkSingletons.findJob(jobId);
    Context firstJobContext = ApacheSparkSingletons.getJobContext(firstJob);

    Integer attemptId = stageInfo.attemptNumber();

    Long submissionTime = (Long) stageInfo.submissionTime().get();

    SpanBuilder builder =
        ApacheSparkSingletons.TRACER
            .spanBuilder("spark_stage")
            .setParent(firstJobContext)
            .setAttribute(SPARK_STAGE_ID_ATTR_KEY, Long.valueOf(stageId))
            .setAttribute(SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY, Long.valueOf(attemptId))
            .setStartTimestamp((Long) stageInfo.submissionTime().get(), TimeUnit.MILLISECONDS);

    for (Object id : JavaConversions.asJavaCollection(stage.jobIds())) {
      Integer jid = (Integer) id;
      if (jid != firstJob.jobId()) {
        ActiveJob job = ApacheSparkSingletons.findJob(jid);
        Context jcontext = ApacheSparkSingletons.getJobContext(job);
        Span s = Span.fromContext(jcontext);
        builder.addLink(s.getSpanContext());
      }
    }
    Span stageSpan = builder.startSpan();
    Context stageContext = stageSpan.storeInContext(firstJobContext);
    ApacheSparkSingletons.setStageContext(stage, stageContext);

    emitSparkEvent(event, submissionTime);
  }

  private static void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();
    stageInfo.completionTime();
    Stage stage = ApacheSparkSingletons.findStage(stageId);
    Context stageContext = ApacheSparkSingletons.getStageContext(stage);

    Long completionTime = (Long) stageInfo.completionTime().get();

    if (stageContext != null) {
      Span span = Span.fromContext(stageContext);
      if (stageInfo.failureReason() instanceof Some) {
        span.setStatus(StatusCode.ERROR, stageInfo.failureReason().get());
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end(completionTime, TimeUnit.MILLISECONDS);
    }
    ApacheSparkSingletons.unregisterStage(stage);

    emitSparkEvent(event, completionTime);
  }

  private static void onTaskStart(SparkListenerTaskStart event) {
    emitSparkEvent(event, event.taskInfo().launchTime());
  }

  private static void onTaskEnd(SparkListenerTaskEnd event) {
    emitSparkEvent(event, event.taskInfo().finishTime());
  }

  private static void onOtherEvent(SparkListenerEvent event) {
    emitSparkEvent(event);
  }

  private static String resolveApplicationNameFromEnvironmentEvent(
      SparkListenerEnvironmentUpdate event) {
    String ret = null;
    Seq<Tuple2<String, String>> properties =
        event.environmentDetails().get("Spark Properties").get();
    Iterator<Tuple2<String, String>> iter = properties.iterator();
    while (iter.hasNext()) {
      Tuple2<String, String> kv = iter.next();
      if ("spark.app.name".equals(kv._1())) {
        ret = kv._2();
        break;
      }
    }
    return ret;
  }

  private static void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {

    if (applicationContext == null) {
      String applicationName = resolveApplicationNameFromEnvironmentEvent(event);
      initApplicationContext(applicationName);
      handlePendingEvents();
    }

    emitSparkEvent(event);
  }

  private static void handlePendingEvents() {
    for (SparkListenerEvent event : PENDING_EVENTS) {
      handleSparkListenerEvent(event);
    }
    PENDING_EVENTS.clear();
  }

  private static void onTaskGettingResult(SparkListenerTaskGettingResult event) {
    emitSparkEvent(event);
  }

  private static void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted event) {
    emitSparkEvent(event);
  }

  private static void onBlockManagerAdded(SparkListenerBlockManagerAdded event) {
    emitSparkEvent(event, event.time());
  }

  private static void onBlockManagerRemoved(SparkListenerBlockManagerRemoved event) {
    emitSparkEvent(event, event.time());
  }

  private static void onUnpersistRDD(SparkListenerUnpersistRDD event) {
    emitSparkEvent(event);
  }

  private static void onExecutorAdded(SparkListenerExecutorAdded event) {
    emitSparkEvent(event, event.time());
  }

  private static void onExecutorRemoved(SparkListenerExecutorRemoved event) {
    emitSparkEvent(event, event.time());
  }

  private static void onExecutorBlacklisted(SparkListenerExecutorBlacklisted event) {
    emitSparkEvent(event, event.time());
  }

  private static void onExecutorBlacklistedForStage(
      SparkListenerExecutorBlacklistedForStage event) {
    emitSparkEvent(event, event.time());
  }

  private static void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage event) {
    emitSparkEvent(event, event.time());
  }

  private static void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted event) {
    emitSparkEvent(event, event.time());
  }

  private static void onNodeBlacklisted(SparkListenerNodeBlacklisted event) {
    emitSparkEvent(event, event.time());
  }

  private static void onNodeUnblacklisted(SparkListenerNodeUnblacklisted event) {
    emitSparkEvent(event, event.time());
  }

  private static void onBlockUpdated(SparkListenerBlockUpdated event) {
    emitSparkEvent(event);
  }

  private static void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate event) {
    // TODO: Better if emit this as metrics?
    emitSparkEvent(event);
  }

  private static void onLogStart(SparkListenerLogStart event) {
    emitSparkEvent(event);
  }

  public static void handleSparkListenerEvent(SparkListenerEvent event) {

    if (applicationContext == null) {
      PENDING_EVENTS.add(event);
    }

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
    } else if (event instanceof SparkListenerEnvironmentUpdate) {
      SparkEventListener.onEnvironmentUpdate((SparkListenerEnvironmentUpdate) event);
    } else if (event instanceof SparkListenerTaskGettingResult) {
      SparkEventListener.onTaskGettingResult((SparkListenerTaskGettingResult) event);
    } else if (event instanceof SparkListenerSpeculativeTaskSubmitted) {
      SparkEventListener.onSpeculativeTaskSubmitted((SparkListenerSpeculativeTaskSubmitted) event);
    } else if (event instanceof SparkListenerBlockManagerAdded) {
      SparkEventListener.onBlockManagerAdded((SparkListenerBlockManagerAdded) event);
    } else if (event instanceof SparkListenerBlockManagerRemoved) {
      SparkEventListener.onBlockManagerRemoved((SparkListenerBlockManagerRemoved) event);
    } else if (event instanceof SparkListenerUnpersistRDD) {
      SparkEventListener.onUnpersistRDD((SparkListenerUnpersistRDD) event);
    } else if (event instanceof SparkListenerExecutorAdded) {
      SparkEventListener.onExecutorAdded((SparkListenerExecutorAdded) event);
    } else if (event instanceof SparkListenerExecutorRemoved) {
      SparkEventListener.onExecutorRemoved((SparkListenerExecutorRemoved) event);
    } else if (event instanceof SparkListenerExecutorBlacklisted) {
      SparkEventListener.onExecutorBlacklisted((SparkListenerExecutorBlacklisted) event);
    } else if (event instanceof SparkListenerExecutorBlacklistedForStage) {
      SparkEventListener.onExecutorBlacklistedForStage(
          (SparkListenerExecutorBlacklistedForStage) event);
    } else if (event instanceof SparkListenerNodeBlacklistedForStage) {
      SparkEventListener.onNodeBlacklistedForStage((SparkListenerNodeBlacklistedForStage) event);
    } else if (event instanceof SparkListenerExecutorUnblacklisted) {
      SparkEventListener.onExecutorUnblacklisted((SparkListenerExecutorUnblacklisted) event);
    } else if (event instanceof SparkListenerNodeBlacklisted) {
      SparkEventListener.onNodeBlacklisted((SparkListenerNodeBlacklisted) event);
    } else if (event instanceof SparkListenerNodeUnblacklisted) {
      SparkEventListener.onNodeUnblacklisted((SparkListenerNodeUnblacklisted) event);
    } else if (event instanceof SparkListenerBlockUpdated) {
      SparkEventListener.onBlockUpdated((SparkListenerBlockUpdated) event);
    } else if (event instanceof SparkListenerExecutorMetricsUpdate) {
      SparkEventListener.onExecutorMetricsUpdate((SparkListenerExecutorMetricsUpdate) event);
    } else if (event instanceof SparkListenerLogStart) {
      SparkEventListener.onLogStart((SparkListenerLogStart) event);
    } else {
      SparkEventListener.onOtherEvent(event);
    }
  }
}
