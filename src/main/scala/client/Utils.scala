package client

import java.io.IOException
import java.net.ServerSocket

object Utils {
  final val noLeadgerId: Int = -1

  def bytesToLongsArray(bytes: Array[Byte]): Array[Long] = {
    val buffer = java.nio.ByteBuffer
      .wrap(bytes)
      .asLongBuffer()

    val size = buffer.limit()
    val longs = {
      if (size == 0) {
        Array.emptyLongArray
      }
      else {
        val array = Array[Long](size)
        buffer.get(array)
        array
      }
    }
    longs
  }


  def longArrayToBytes(longs: Array[Long]): Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      longs.length * java.lang.Long.BYTES
    )
    longs.foreach(longValue => buffer.putLong(longValue))
    buffer.array()
  }

  def bytesToIntsArray(bytes: Array[Byte]): Array[Int] = {
    val buffer = java.nio.ByteBuffer
      .wrap(bytes)
      .asIntBuffer()

    val size = buffer.limit()
    val ints = {
      if (size == 0) {
        Array.emptyIntArray
      }
      else {
        val array = Array[Int](size)
        buffer.get(array)
        array
      }
    }
    ints
  }

  def getRandomPort: Int = {
    scala.util.Try {
      val server = new ServerSocket(0)
      val port = server.getLocalPort
      server.close()
      port
    } match {
      case scala.util.Success(port) =>
        port
      case scala.util.Failure(throwable: IOException) =>
        throw throwable
    }
  }
}
