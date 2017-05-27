import org.apache.bookkeeper.util.LocalBookKeeper

object BookieServerStartup extends App {
  lazy val localBookKeeperNumber = 5
  LocalBookKeeper.main(Array(localBookKeeperNumber.toString))
}



