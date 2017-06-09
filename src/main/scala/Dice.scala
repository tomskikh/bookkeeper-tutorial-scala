import java.io.Closeable
import java.util.concurrent.TimeUnit

import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import client.{EntryId, LeaderRole}
import client.master.Master
import client.slave.Slave
import org.apache.bookkeeper.meta.{FlatLedgerManagerFactory, HierarchicalLedgerManagerFactory}
import org.apache.log4j.{Level, Logger}


class Dice
  extends Closeable
{

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

  private val leaderSelector = new LeaderRole(client, Dice.ELECTION_PATH)

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
    Dice.DICE_PASSWORD
  )

  private val slave = new Slave(
    client,
    bookKeeper,
    leaderSelector,
    Dice.DICE_LOG,
    Dice.DICE_PASSWORD
  )

  def playDice(): Unit = {
    var lastDisplayedEntry = EntryId(-1L, -1L)
    while (true) {
      if (leaderSelector.hasLeadership) {
        lastDisplayedEntry = master.lead(lastDisplayedEntry)
      } else {
        lastDisplayedEntry = slave.follow(lastDisplayedEntry)
      }
    }
  }

  override def close(): Unit = {
    bookKeeper.close()
    leaderSelector.close()
    client.close()
  }
}

private object Dice{
  val ELECTION_PATH: String = "/dice-elect"
  val DICE_PASSWORD: Array[Byte] = "dice".getBytes
  val DICE_LOG: String = "/dice-log"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.OFF)
    val dice = new Dice()
    try {
      dice.playDice()
    }
    finally {
      dice.close()
    }
  }
}
