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

package org.dist.queue.network

import java.io.{DataInputStream, DataOutputStream}
import java.net._

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.Logging
import org.dist.queue.server.KafkaApis
import org.dist.queue.utils.Utils
import org.dist.util.SocketIO

class SocketServer(val brokerId: Int,
                   val host: String,
                   val port: Int,
                   val numProcessorThreads: Int = 2,
                   val maxQueuedRequests: Int = 500,
                   val sendBufferSize: Int = 100*1024,
                   val recvBufferSize: Int = 100*1024,
                   val maxRequestSize: Int = Int.MaxValue) extends Logging {

  var listener:TcpListener = null

  /**
   * Start the socket server
   */
  def startup(kafkaApis: KafkaApis) {
    listener = new TcpListener(InetAddressAndPort.create(host, port), kafkaApis, this)
    listener.start()
    info("Started socket server")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    listener.shudown()
    info("Shutdown completed")
  }

  def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort) = {
    val clientSocket = new Socket(to.address, to.port)
    new SocketIO[RequestOrResponse](clientSocket, classOf[RequestOrResponse]).requestResponse(message)
  }
}

class TcpListener(localEp: InetAddressAndPort, kafkaApis: KafkaApis, socketServer: SocketServer) extends Thread with Logging {
  var serverSocket:ServerSocket = null

  def shudown() = {
    Utils.swallow(serverSocket.close())
  }


  override def run(): Unit = {
    serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(localEp.address, localEp.port))
    info(s"Listening on ${localEp}")
    while (true) {
      val socket = serverSocket.accept()
      socket.setSoTimeout(1000)
      val inputStream = socket.getInputStream()
      trace(s"Connection from ${socket.getInetAddress}, ${socket.getPort}" )

      val dataInputStream = new DataInputStream(inputStream)
      val size = dataInputStream.readInt()
      val messageBytes = new Array[Byte](size)
      inputStream.read(messageBytes)
      val request = JsonSerDes.deserialize(messageBytes, classOf[RequestOrResponse])

      val response = kafkaApis.handle(request)
      val str = JsonSerDes.serialize(response)

      val outptStream = socket.getOutputStream
      val dataOutputStream = new DataOutputStream(outptStream)
      val bytes = str.getBytes()
      dataOutputStream.writeInt(bytes.size)
      dataOutputStream.write(bytes)
      outptStream.flush()
      outptStream.close()
      inputStream.close()
      socket.close()
    }
  }
}