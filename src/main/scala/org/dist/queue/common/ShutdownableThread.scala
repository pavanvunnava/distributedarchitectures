/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dist.queue.common

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)
        extends Thread(name) with Logging {
  this.setDaemon(false)
  this.logIdent = "[" + name + "], "
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)


  def shutdown(): Unit = {
    info("Shutting down")
    isRunning.set(false)
    if (isInterruptible)
      interrupt()
    shutdownLatch.await()
    info("Shutdown completed")
  }

    /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def doWork(): Unit

  override def run(): Unit = {
    info("Starting ")
    try{
      while(isRunning.get()){
        doWork()
      }
    } catch{
      case e: Throwable =>
        if(isRunning.get())
          error("Error due to ", e)
    }
    shutdownLatch.countDown()
    info("Stopped ")
  }
}