/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023,2024 Key Tiong TAN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons;
import io.opentelemetry.javaagent.instrumentation.spark.SparkEventLogger;
import java.util.concurrent.TimeUnit;
import org.apache.spark.scheduler.*;
import scala.collection.JavaConversions;

public class SparkEventListener {

  private static final AttributeKey<Long> SPARK_JOB_ID_ATTR_KEY =
      AttributeKey.longKey("spark.job_id");

  private static final AttributeKey<Long> SPARK_STAGE_ID_ATTR_KEY =
      AttributeKey.longKey("spark.stage_id");

  private static final AttributeKey<Long> SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.stage_attempt_number");

  public static void onApplicationStart(SparkListenerApplicationStart event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  public static void onApplicationEnd(SparkListenerApplicationEnd event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
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
            .startSpan();
    Context jobContext = jobSpan.storeInContext(parentContext);

    ApacheSparkSingletons.setJobContext(job, jobContext);

    SparkEventLogger.emitSparkEvent(event, event.time());
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

    SparkEventLogger.emitSparkEvent(event, event.time());
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

    SparkEventLogger.emitSparkEvent(event, submissionTime);
  }

  private static void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();
    Stage stage = ApacheSparkSingletons.findStage(stageId);
    Context stageContext = ApacheSparkSingletons.getStageContext(stage);

    Long completionTime = (Long) stageInfo.completionTime().get();

    if (stageContext != null) {
      Span span = Span.fromContext(stageContext);
      String status = stageInfo.getStatusString();
      if ("failed".equals(status)) {
        span.setStatus(StatusCode.ERROR, stageInfo.failureReason().get());
      } else if ("succeeded".equals(status)) {
        span.setStatus(StatusCode.OK);
        ApacheSparkSingletons.unregisterStage(stage);
      }
      span.end(completionTime, TimeUnit.MILLISECONDS);
    }

    SparkEventLogger.emitSparkEvent(event, completionTime);
  }

  private static void onTaskStart(SparkListenerTaskStart event) {
    SparkEventLogger.emitSparkEvent(event, event.taskInfo().launchTime());
  }

  private static void onTaskEnd(SparkListenerTaskEnd event) {
    SparkEventLogger.emitSparkEvent(event, event.taskInfo().finishTime());
  }

  private static void onOtherEvent(SparkListenerEvent event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onTaskGettingResult(SparkListenerTaskGettingResult event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onBlockManagerAdded(SparkListenerBlockManagerAdded event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  private static void onBlockManagerRemoved(SparkListenerBlockManagerRemoved event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  private static void onUnpersistRDD(SparkListenerUnpersistRDD event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onExecutorAdded(SparkListenerExecutorAdded event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  private static void onExecutorRemoved(SparkListenerExecutorRemoved event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onExecutorBlacklisted(SparkListenerExecutorBlacklisted event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onExecutorBlacklistedForStage(
      SparkListenerExecutorBlacklistedForStage event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onNodeBlacklisted(SparkListenerNodeBlacklisted event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  @SuppressWarnings("deprecation")
  private static void onNodeUnblacklisted(SparkListenerNodeUnblacklisted event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  private static void onBlockUpdated(SparkListenerBlockUpdated event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate event) {
    // TODO: Better if emit this as metrics?
    SparkEventLogger.emitSparkEvent(event);
  }

  private static void onLogStart(SparkListenerLogStart event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  @SuppressWarnings("deprecation")
  public static void handleSparkListenerEvent(SparkListenerEvent event) {
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
