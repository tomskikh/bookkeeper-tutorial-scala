import java.io.File

import client.Utils
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory
import org.apache.bookkeeper.proto.BookieServer
import org.apache.commons.io.FileUtils

object BookieServerStartup {
  val localBookKeeperNumber = 5
  val zkLedgersRootPath = "/ledgers"
  val zkBookiesAvailablePath = s"$zkLedgersRootPath/available"

  private def createBookieFolder(prefix: String) = {
    val bookieFolder =
      new File(s"/tmp/bookie$prefix", "current")

    bookieFolder.mkdir()

    bookieFolder.getPath
  }


  final def startBookie(bookieNumber: Int): (BookieServer, String) = {
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

    (server, bookieFolder)
  }

  private def addShutdownHook(servers: Seq[BookieServer], folders: Seq[String]) = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        servers.foreach(_.shutdown())

        folders.foreach(x => FileUtils.deleteDirectory(new File(x).getParentFile))
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val (servers, folders) = (0 until localBookKeeperNumber map startBookie).unzip

    addShutdownHook(servers, folders)
  }
}



