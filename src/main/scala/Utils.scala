import java.io.IOException
import java.net.ServerSocket

object Utils {
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
