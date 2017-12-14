/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tracer

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import brave.{Span, Tracing}
import zipkin2.reporter.AsyncReporter
import zipkin2.reporter.okhttp3.OkHttpSender

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


/**
  * @author Pavel Tomskikh
  */
class Tracer(zipkinAddress: String, serviceName: String, rootName: String) {

  private implicit val executionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

  private val sender = OkHttpSender.create(s"http://$zipkinAddress/api/v2/spans")
  private val reporter = AsyncReporter.create(sender)
  private val tracing = Tracing.newBuilder()
    .localServiceName(serviceName)
    .spanReporter(reporter)
    .build()
  private val tracer = tracing.tracer()

  private val spans = TrieMap.empty[String, Span]
  private val root = tracer.newTrace().name(rootName)
  private val started = new AtomicBoolean(false)
  Future()

  def start(): Unit = {
    val timestamp = Clock.currentTimeMicroseconds
    Future {
      if (!started.getAndSet(true)) root.start(timestamp)
    }
  }

  def start(name: String, parentName: Option[String] = None): Future[Unit] = {
    val timestamp = Clock.currentTimeMicroseconds
    Future {
      if (!started.getAndSet(true)) root.start(timestamp)
      val parent = parentName.flatMap(n => spans.get(n)).getOrElse(root)
      val span = tracer.newChild(parent.context()).name(name).start(timestamp)
      spans += name -> span
    }
  }

  def finish(name: String): Future[Unit] = {
    val timestamp = Clock.currentTimeMicroseconds
    Future(spans.remove(name).foreach(_.finish(timestamp)))
  }


  def closeAsync(): Future[Unit] = {
    val timestamp = Clock.currentTimeMicroseconds
    Future {
      spans.values.foreach(_.finish(timestamp))
      spans.clear()
      root.finish(timestamp)
      tracing.close()
      reporter.close()
      sender.close()
      println("tracer closed")
      executionContext.shutdown()
    }
  }

  def close(): Unit =
    Await.result(closeAsync(), Duration.Inf)
}