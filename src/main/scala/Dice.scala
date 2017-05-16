import java.io.Closeable
import java.util.concurrent.TimeUnit

import org.apache.bookkeeper.client.{BKException, BookKeeper}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.apache.zookeeper.data.Stat

import scala.annotation.tailrec

class Dice
  extends LeaderSelectorListenerAdapter
    with Closeable
{
  private val zkServer = new TestingServer(true)

  println(zkServer.getConnectString)
  private val client = {
    val connection = CuratorFrameworkFactory.builder()
      .connectString(zkServer.getConnectString)
      .sessionTimeoutMs(2000)
      .connectionTimeoutMs(5000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build()

    connection.start()
    connection.blockUntilConnected(5000, TimeUnit.MILLISECONDS)
    connection
  }

  private val leaderSelector = {
    val leader = new LeaderSelector(client, Dice.ELECTION_PATH, this)
    leader.autoRequeue()
    leader.start()

    leader
  }


  private val bookKeeper = {
    val configuration = new ClientConfiguration()
      .setZkServers(zkServer.getConnectString)
      .setZkTimeout(30000)

    client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath("/ledgers/available", Array.emptyByteArray)
    configuration.setZkLedgersRootPath("/ledgers")

    new BookKeeper(configuration)
  }

  private val rand = scala.util.Random
  private def bytesToLongsArray(bytes: Array[Byte]) = {
    java.nio.ByteBuffer
      .wrap(bytes)
      .asLongBuffer()
      .array()
  }

  def initLedgers: (Array[Long], Stat, Boolean) = {
    scala.util.Try {
      val stat: Stat = new Stat()
      val ledgerListBytes = client.getData
        .storingStatIn(stat)
        .forPath(Dice.DICE_LOG)

      (bytesToLongsArray(ledgerListBytes), stat)
    } match {
      case scala.util.Success((ledgers, stat)) =>
        (ledgers, stat ,false)
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          (Array.emptyLongArray, new Stat(), true)
        case _ => throw throwable
      }
    }
  }

  private def bytesToIntsArray(bytes: Array[Byte]) = {
    java.nio.ByteBuffer
      .wrap(bytes)
      .asIntBuffer()
      .array()
  }

  private def longArrayToBytes(longs: Array[Long]): Array[Byte] = {
    val buffer = java.nio.ByteBuffer.allocate(
      longs.length * java.lang.Long.BYTES
    )
    longs.foreach(longValue => buffer.putLong(longValue))
    buffer.array()
  }

  def lead(skipPast: EntryId): EntryId = {
    val (ledgerIDs, stat, mustCreate) = initLedgers

    val toRead = {
      if (skipPast.ledgerId != -1)
        ledgerIDs.takeRight(ledgerIDs.indexOf(skipPast.ledgerId))
      else
        Array.emptyLongArray
    }.toStream

    val ledgersHandlerToDisplay = toRead
      .map(leadgerID =>
        scala.util.Try(
          bookKeeper.openLedger(
            leadgerID,
            BookKeeper.DigestType.MAC,
            Dice.DICE_PASSWORD
          )))
      .takeWhile {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) => throwable match {
          case _: BKException.BKLedgerRecoveryException => false
          case _: Throwable => throw throwable
        }
      }.toArray

    var lastDisplayedEntry = skipPast
    val nextEntry = EntryId(skipPast.ledgerId, skipPast.entryId + 1)
    ledgersHandlerToDisplay.foreach { case (tryOpenLedgerHandle) =>
      val ledgeHanlder = tryOpenLedgerHandle.get
      if (nextEntry.entryId > ledgeHanlder.getLastAddConfirmed) {
        lastDisplayedEntry = EntryId(skipPast.ledgerId, 0)
      } else {
        val entries = ledgeHanlder.readEntries(
          nextEntry.entryId,
          ledgeHanlder.getLastAddConfirmed
        )
        while (entries.hasMoreElements) {
          val entry = entries.nextElement
          val entryData = entry.getEntry
          println(s"" +
            s"Value = ${bytesToIntsArray(entryData)}, " +
            s"epoch = ${ledgeHanlder.getId}, " +
            "catchup"
          )
          lastDisplayedEntry = EntryId(ledgeHanlder.getId, entry.getEntryId)
        }
      }
    }

    val ensembleNumber = 3
    val writeQourumNumber = 3
    val ackQourumNumber = 2
    val ledgerHandle = bookKeeper.createLedger(
      ensembleNumber,
      writeQourumNumber,
      ackQourumNumber,
      BookKeeper.DigestType.MAC,
      Dice.DICE_PASSWORD
    )

    val ledgersIDsToBytes = longArrayToBytes(ledgerIDs :+ ledgerHandle.getId)
    if (mustCreate) {
      scala.util.Try(
        client.create.forPath(Dice.DICE_LOG, ledgersIDsToBytes)
      ) match {
        case scala.util.Success(_) =>
        case scala.util.Failure(throwable) => throwable match {
          case _: KeeperException.NodeExistsException =>
          case _ => throw throwable
        }
      }
    } else {
      scala.util.Try(
        client.setData()
          .withVersion(stat.getVersion)
          .forPath(Dice.DICE_LOG, ledgersIDsToBytes)
      ) match {
        case scala.util.Success(_) =>
        case scala.util.Failure(throwable) => throwable match {
          case _: KeeperException.BadVersionException =>
          case _ => throw throwable
        }
      }
    }

    try {
      while (leaderSelector.hasLeadership) {
        Thread.sleep(1000)
        val nextInt = rand.nextInt(6) + 1
        ledgerHandle.addEntry(
          java.nio.ByteBuffer.allocate(4).putInt(nextInt).array()
        )
        println(
          s"Value = $nextInt, " +
            s"epoch = ${ledgerHandle.getId}, " +
            s"isLeader = ${leaderSelector.hasLeadership}"
        )
      }
    } finally {
      ledgerHandle.close()
    }

    lastDisplayedEntry
  }

  @tailrec
  private final def ledgersIDs(entryId: EntryId): Seq[Long] = {
    scala.util.Try {
      val ledgerListBytes = client.getData
        .forPath(Dice.DICE_LOG)
      val ids: Seq[Long] =
        if (entryId.ledgerId != -1) {
          bytesToLongsArray(ledgerListBytes)
            .takeRight(ledgerListBytes.indexOf(entryId.ledgerId))
        } else {
          Seq.empty[Long]
        }
      ids
    } match {
      case scala.util.Success(ledgersIDs) =>
        ledgersIDs
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          Thread.sleep(1000)
          ledgersIDs(entryId)
        case _ =>
          throw throwable
      }
    }
  }

  private def readUntilClosed(ledgerIDs: Seq[Long], lastReadEntry: EntryId): EntryId = {
    @tailrec
    def go(ids: Seq[Long],
           isClosed: Boolean,
           nextEntry: EntryId,
           lastReadEntry: EntryId
          ): EntryId = {
      if (ids.isEmpty) {
        lastReadEntry
      }
      else if (isClosed || leaderSelector.hasLeadership) {
        go(
          ids.tail,
          isClosed = false,
          EntryId(nextEntry.ledgerId, entryId = 0L),
          lastReadEntry
        )
      }
      else {
        val currentHandle = ids.head
        val nextEntry_rename =
          if (lastReadEntry.ledgerId == currentHandle)
            nextEntry.copy(entryId = lastReadEntry.entryId + 1)
          else
            nextEntry

        val isClosed_rename = bookKeeper.isClosed(currentHandle)
        val ledgerHandle = bookKeeper.openLedgerNoRecovery(
          currentHandle,
          BookKeeper.DigestType.MAC,
          Dice.DICE_PASSWORD
        )

        var lastReadEntry_rename = lastReadEntry
        if (nextEntry_rename.entryId <= ledgerHandle.getLastAddConfirmed) {
          val entries = ledgerHandle.readEntries(
            nextEntry_rename.entryId,
            ledgerHandle.getLastAddConfirmed
          )

          while (entries.hasMoreElements) {
            val entry = entries.nextElement()
            val entryData = entry.getEntry
            println(
              s"Value = ${bytesToIntsArray(entryData)}, " +
                s"epoch = ${ledgerHandle.getId}, " +
                "following"
            )
            lastReadEntry_rename = EntryId(currentHandle, entry.getEntryId)
          }
        }
        Thread.sleep(1000)
        go(
          ids.tail,
          isClosed_rename,
          nextEntry_rename,
          lastReadEntry_rename
        )
      }
    }

    go(
      ledgerIDs,
      isClosed = false,
      EntryId(lastReadEntry.ledgerId, entryId = 0L),
      lastReadEntry
    )
  }

  @tailrec
  private final def readNewLedgers(ledgers: Seq[Long], lastReadEntry: EntryId): EntryId = {
    if (leaderSelector.hasLeadership) {
      val lastReadEntry_rename = readUntilClosed(ledgers, lastReadEntry)

      val newLedgersBinaryIDs = client.getData
        .forPath(Dice.DICE_LOG)

      val ledgersIDs = bytesToLongsArray(newLedgersBinaryIDs)

      readNewLedgers(
        ledgersIDs.takeRight(
          ledgersIDs.indexOf(lastReadEntry_rename.ledgerId + 1)
        ),
        lastReadEntry_rename
      )
    } else {
      lastReadEntry
    }
  }

  def follow(skipPast: EntryId): EntryId = {
    val ledgers = ledgersIDs(skipPast)
    readNewLedgers(ledgers,  skipPast)
  }

  def playDice(): Unit = {
    var lastDisplayedEntry = EntryId(-1L, -1L)
    while (true) {
      if (leaderSelector.hasLeadership) {
        lastDisplayedEntry = lead(lastDisplayedEntry)
      } else {
        lastDisplayedEntry = follow(lastDisplayedEntry)
      }
    }
  }


  @throws[Exception]
  override def takeLeadership(client: CuratorFramework): Unit = {
    this.synchronized {
      println("Becoming leader")
      try {
        while(true) this.wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
  }

  override def close(): Unit = {
    bookKeeper.close()
    leaderSelector.close()
    client.close()
    zkServer.close()
  }
}

private object Dice{
  val ELECTION_PATH: String = "/dice-elect"
  val DICE_PASSWORD: Array[Byte] = "dice".getBytes
  val DICE_LOG: String = "/dice-log"

  def main(args: Array[String]): Unit = {
    val dice = new Dice()
    try {
      dice.playDice()
    }
    finally {
      dice.close()
    }
  }
}
