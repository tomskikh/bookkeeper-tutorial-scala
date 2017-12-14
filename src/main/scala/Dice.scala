import java.io.Closeable
import java.util.concurrent.TimeUnit

import client.master.Master
import client.{EntryId, LeaderSelector}
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.FlatLedgerManagerFactory
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.{Level, Logger}


class Dice(ensembleNumber: Int = 3,
           writeQuorumNumber: Int = 3,
           ackQuorumNumber: Int = 2,
           isSync: Boolean = true,
           from: Int = 1000,
           to: Int = 1020) extends Closeable {

  private val client: CuratorFramework = {
    val connection = CuratorFrameworkFactory.builder()
      .connectString(s"127.0.0.1:${ZookeeperServerStartup.port}")
      .sessionTimeoutMs(2000)
      .connectionTimeoutMs(5000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()

    connection.start()
    connection.blockUntilConnected(5000, TimeUnit.MILLISECONDS)
    connection
  }

  private val leaderSelector = new LeaderSelector(client, Dice.ELECTION_PATH)

  private val bookKeeper: BookKeeper = {
    val configuration = new ClientConfiguration()
      .setZkServers(s"127.0.0.1:${ZookeeperServerStartup.port}")
      .setZkTimeout(30000)

    configuration.setLedgerManagerFactoryClass(
      classOf[FlatLedgerManagerFactory]
    )

    new BookKeeper(configuration)
  }

  private val master = new Master(
    client,
    bookKeeper,
    leaderSelector,
    Dice.DICE_LOG,
    Dice.DICE_PASSWORD,
    ensembleNumber = ensembleNumber,
    writeQuorumNumber = writeQuorumNumber,
    ackQuorumNumber = ackQuorumNumber,
    isSync = isSync,
    to = to)

  def playDice(): Unit = {
    var lastDisplayedEntry = EntryId(-1L, -1L)
    while (!master.isDone.get()) {
      if (leaderSelector.hasLeadership)
        lastDisplayedEntry = master.lead(lastDisplayedEntry)
    }
  }

  override def close(): Unit = {
    bookKeeper.close()
    leaderSelector.close()
    client.close()
  }
}

private object Dice {
  val ELECTION_PATH: String = "/dice-elect"
  val DICE_PASSWORD: Array[Byte] = "dice".getBytes
  val DICE_LOG: String = "/dice-log"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.OFF)

    val bookies = 3

    for {
      ensembleNumber <- 1 to bookies
      writeQuorumNumber <- 1 to ensembleNumber
      ackQuorumNumber <- 1 to writeQuorumNumber
      isSync <- Seq(true, false)
    } yield {
      val dice = new Dice(
        ensembleNumber = ensembleNumber,
        writeQuorumNumber = writeQuorumNumber,
        ackQuorumNumber = ackQuorumNumber,
        isSync = isSync,
        from = 1000,
        to = 1020)
      try {
        dice.playDice()
      }
      finally {
        dice.close()
      }
    }
  }
}
