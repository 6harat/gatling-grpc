package com.github.phisgr.gatling.grpc.stream

import com.github.phisgr.gatling.generic.util.EventLoopHelper
import com.github.phisgr.gatling.grpc.Reflections
import com.github.phisgr.gatling.grpc.stream.StreamCall.Cancelled
import io.gatling.commons.util.Clock
import io.grpc.{ClientCall, Metadata, Status}
import io.netty.channel.EventLoop

class StreamListener[Res](
  state: StreamCall[_, Res, _],
  clock: Clock,
  eventLoop: EventLoop,
  ignoreMessage: Boolean
) extends ClientCall.Listener[Any] {
  private[this] var resHeaders: Metadata = _

  override def onHeaders(headers: Metadata): Unit = {
    this.resHeaders = headers
  }

  override def onMessage(message: Any): Unit = {
    val receiveTime = clock.nowMillis
    if (ignoreMessage) {
      state.call.request(1)
    } else {
      eventLoop.checkAndExecute { () => state.onRes(message, receiveTime) }
    }
  }

  override def onClose(status: Status, trailers: Metadata): Unit = {
    val receiveTime = clock.nowMillis
    if ((status.getCause ne Cancelled) && (status ne Reflections.SHUTDOWN_NOW_STATUS)) {
      eventLoop.checkAndExecute { () => state.onServerCompleted(status, trailers, resHeaders, receiveTime) }
    }
  }
}
