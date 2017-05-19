import org.apache.bookkeeper.util.LocalBookKeeper

object BookieServerStratup extends App {
  lazy val localBookKeeperNumber = 5
  LocalBookKeeper.main(Array(localBookKeeperNumber.toString))
}



