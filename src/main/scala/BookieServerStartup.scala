import java.io.File

import client.Utils
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory
import org.apache.bookkeeper.proto.BookieServer

object BookieServerStartup {
  val localBookKeeperNumber = 5
  val zkLedgersRootPath = "/ledgers"
  val zkBookiesAvailablePath = s"$zkLedgersRootPath/available"

  private def createBookieFolder(prefix: String) = {
    val bookieFolder =
      new File(s"/tmp/bookie${System.currentTimeMillis()}", "current")

    bookieFolder.mkdir()

    bookieFolder.getPath
  }


  final def startBookie(bookieNumber: Int): Unit = {
    val bookieFolder = createBookieFolder(Integer.toString(bookieNumber))

    val serverConfig = new ServerConfiguration()
      .setBookiePort(Utils.getRandomPort)
      .setZkServers(ZookeeperServerStartup.endpoints)
      .setJournalDirName(bookieFolder)
      .setLedgerDirNames(Array(bookieFolder))
      .setAllowLoopback(true)

    serverConfig.setLedgerManagerFactoryClass(
      classOf[FlatLedgerManagerFactory]
    )

    val server = new BookieServer(serverConfig)
    server.start()
  }

  def main(args: Array[String]): Unit = {
    0 until localBookKeeperNumber foreach startBookie
  }
}



