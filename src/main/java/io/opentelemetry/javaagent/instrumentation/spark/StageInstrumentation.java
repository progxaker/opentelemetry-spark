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
