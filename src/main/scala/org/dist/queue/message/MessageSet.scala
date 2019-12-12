package org.dist.queue.message

import java.nio._
import java.nio.channels._

/**
 * Message set helper functions
 */
object MessageSet {

  val MessageSizeLength = 4
  val OffsetLength = 8
  val LogOverhead = MessageSizeLength + OffsetLength
  val Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0))

  /**
   * The size of a message set containing the given messages
   */
  def messageSetSize(messages: Iterable[Message]): Int =
    messages.foldLeft(0)(_ + entrySize(_))

  /**
   * The size of a list of messages
   */
  def messageSetSize(messages: java.util.List[Message]): Int = {
    var size = 0
    val iter = messages.iterator
    while(iter.hasNext) {
      val message = iter.next.asInstanceOf[Message]
      size += entrySize(message)
    }
    size
  }

  /**
   * The size of a size-delimited entry in a message set
   */
  def entrySize(message: Message): Int = LogOverhead + message.size

}

/**
 * A set of messages with offsets. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. The format of each message is
 * as follows:
 * 8 byte message offset number
 * 4 byte size containing an integer N
 * N message bytes as described in the Message class
 */
abstract class MessageSet extends Iterable[MessageAndOffset] {

  /** Write the messages in this set to the given channel starting at the given offset byte.
   * Less than the complete amount may be written, but no more than maxSize can be. The number
   * of bytes written is returned */
  def writeTo(channel: GatheringByteChannel, offset: Long, maxSize: Int): Int

  /**
   * Provides an iterator over the message/offset pairs in this set
   */
  def iterator: Iterator[MessageAndOffset]

  /**
   * Gives the total size of this message set in bytes
   */
  def sizeInBytes: Int

  /**
   * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
   * match the payload for any message.
   */
  def validate(): Unit = {
    for(messageAndOffset <- this)
      if(!messageAndOffset.message.isValid)
        throw new RuntimeException("Invalid message")
  }

  /**
   * Print this message set's contents
   */
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(getClass.getSimpleName + "(")
    for(message <- this) {
      builder.append(message)
      builder.append(", ")
    }
    builder.append(")")
    builder.toString
  }

}