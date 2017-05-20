import java.io.Closeable
import java.util.concurrent.TimeUnit

import org.apache.bookkeeper.client.{BKException, BookKeeper, LedgerHandle}
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat

import scala.annotation.tailrec
import org.apache.bookkeeper.client.BookKeeper.DigestType

import Dice._
import Utils._


class Dice
  extends LeaderSelectorListenerAdapter
    with Closeable {
  private val client = {
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

  private val leaderSelector = {
    val leader = new LeaderSelector(client, Dice.ELECTION_PATH, this)
    leader.autoRequeue()
    leader.start()

    leader
  }


  private val bookKeeper = {
    val configuration = new ClientConfiguration()
      .setZkServers(s"127.0.0.1:${ZookeeperServerStartup.port}")
      .setZkTimeout(30000)

    //    client.create()
    //      .creatingParentsIfNeeded()
    //      .withMode(CreateMode.PERSISTENT)
    //      .forPath("/ledgers/available", Array.emptyByteArray)
    //    configuration.setZkLedgersRootPath("/ledgers")

    new BookKeeper(configuration)
  }


  private def retrieveAllLedgersFromZkServer(path: String): LedgersWithMetadataInformation = {
    val zNodeMetadata: Stat = new Stat()
    scala.util.Try {
      val binaryData = client.getData
        .storingStatIn(zNodeMetadata)
        .forPath(path)
      val ledgers = bytesToLongsArray(binaryData)

      ledgers
    } match {
      case scala.util.Success(ledgers) =>
        LedgersWithMetadataInformation(ledgers, zNodeMetadata, mustCreate = false)
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          LedgersWithMetadataInformation(Array.emptyLongArray, zNodeMetadata, mustCreate = true)
        case _ =>
          throw throwable
      }
    }
  }

  private def processNewLedgersThatHaventSeenBefore(ledgers: Array[Long],
                                                    skipPast: EntryId) = {
    if (skipPast.ledgerId != noLeadgerId)
      ledgers.takeRight(ledgers.indexOf(skipPast.ledgerId))
    else
      ledgers
  }


  private def openLedgersHandlers(ledgers: Stream[Long],
                                  digestType: DigestType,
                                  password: Array[Byte]) = {
    ledgers
      .map(ledgerID =>
        scala.util.Try(
          bookKeeper.openLedger(
            ledgerID,
            digestType,
            password
          )))
      .takeWhile {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) => throwable match {
          case _: BKException.BKLedgerRecoveryException => false
          case _: Throwable => throw throwable
        }
      }
      .map(_.get).toArray
  }

  @tailrec
  private def traverseLedgersRecords(ledgerHandlers: List[LedgerHandle],
                                     nextEntry: EntryId,
                                     lastDisplayedEntry: EntryId
                                    ): EntryId =
    ledgerHandlers match {
      case Nil =>
        lastDisplayedEntry

      case ledgeHandle :: handles =>
        if (nextEntry.entryId > ledgeHandle.getLastAddConfirmed) {
          val startEntry = EntryId(ledgeHandle.getId, 0)
          traverseLedgersRecords(handles, startEntry, lastDisplayedEntry)
        }
        else {
          val entries = ledgeHandle.readEntries(
            nextEntry.entryId,
            ledgeHandle.getLastAddConfirmed
          )

          var newLastDisplayedEntry = lastDisplayedEntry
          while (entries.hasMoreElements) {
            val entry = entries.nextElement
            val entryData = entry.getEntry
            println(s"" +
              s"Ledger = ${ledgeHandle.getId}, " +
              s"RecordID = ${entry.getEntryId}, " +
              s"Value = ${bytesToIntsArray(entryData).head}, " +
              "catchup"
            )
            newLastDisplayedEntry = EntryId(ledgeHandle.getId, entry.getEntryId)
          }
          traverseLedgersRecords(handles, nextEntry, newLastDisplayedEntry)
        }
    }

  private def ledgerHandleToWrite(ensembleNumber: Int,
                                  writeQuorumNumber: Int,
                                  ackQuorumNumber: Int,
                                  digestType: DigestType,
                                  password: Array[Byte]
                                 ) = {
    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      password
    )
  }


  private def createLedgersLog(path: String,
                               ledgersIDsBinary: Array[Byte]
                              ) = {
    scala.util.Try(
      client.create.forPath(path, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NodeExistsException =>
        case _ => throw throwable
      }
    }
  }

  private def updateLedgersLog(path: String,
                               ledgersIDsBinary: Array[Byte],
                               zNodeMetadata: Stat
                              ) = {
    scala.util.Try(
      client.setData()
        .withVersion(zNodeMetadata.getVersion)
        .forPath(path, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.BadVersionException =>
        case _ => throw throwable
      }
    }
  }

  private final def whileLeaderDo(ledgerHandle: LedgerHandle,
                                  onBeingLeaderDo: LedgerHandle => Unit
                                 ) = {
    try {
      while (leaderSelector.hasLeadership) {
        onBeingLeaderDo(ledgerHandle)
      }
    } finally {
      ledgerHandle.close()
    }
  }

  private val rand = scala.util.Random

  private def onBeingLeaderDo(ledgerHandle: LedgerHandle) = {
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

  def lead(skipPast: EntryId): EntryId = {
    val ledgersWithMetadataInformation =
      retrieveAllLedgersFromZkServer(Dice.DICE_LOG)

    val (ledgerIDs, stat, mustCreate) = (
      ledgersWithMetadataInformation.ledgers,
      ledgersWithMetadataInformation.zNodeMetadata,
      ledgersWithMetadataInformation.mustCreate
    )

    val newLedgers: Stream[Long] =
      processNewLedgersThatHaventSeenBefore(ledgerIDs, skipPast)
        .toStream

    val newLedgerHandles: List[LedgerHandle] =
      openLedgersHandlers(newLedgers, BookKeeper.DigestType.MAC, Dice.DICE_PASSWORD)
        .toList

    val lastDisplayedEntry: EntryId =
      traverseLedgersRecords(newLedgerHandles, EntryId(skipPast.ledgerId, skipPast.entryId + 1), skipPast)


    val ensembleNumber = 3
    val writeQuorumNumber = 3
    val ackQuorumNumber = 2
    val ledgerHandle = ledgerHandleToWrite(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      BookKeeper.DigestType.MAC,
      Dice.DICE_PASSWORD
    )

    val ledgersIDsToBytes = longArrayToBytes(ledgerIDs :+ ledgerHandle.getId)
    if (mustCreate) {
      createLedgersLog(Dice.DICE_LOG, ledgersIDsToBytes)
    } else {
      updateLedgersLog(Dice.DICE_LOG, ledgersIDsToBytes, stat)
    }

    whileLeaderDo(ledgerHandle, onBeingLeaderDo)

    lastDisplayedEntry
  }


  @tailrec
  private final def retrieveLedgersUntilNodeDoesntExist(path: String,
                                                        lastLedgerAndItsLastRecordSeen: EntryId
                                                       ): Array[Long] = {
    scala.util.Try {
      val ledgerIDsBinary = client.getData
        .forPath(path)

      val ledgers = bytesToLongsArray(ledgerIDsBinary)

      processNewLedgersThatHaventSeenBefore(ledgers, lastLedgerAndItsLastRecordSeen)
    } match {
      case scala.util.Success(ledgers) =>
        ledgers
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          Thread.sleep(1000)
          retrieveLedgersUntilNodeDoesntExist(path, lastLedgerAndItsLastRecordSeen)
        case _ =>
          throw throwable
      }
    }
  }

  @tailrec
  private final def monitorLedgerUntilItIsCompleted(ledger: Long,
                                                    lastLedgerAndItsLastRecordSeen: EntryId,
                                                    recordToReadToStartWith: Long,
                                                    isLedgerCompleted: Boolean
                                                   ): EntryId = {
    if (isLedgerCompleted || leaderSelector.hasLeadership) {
      lastLedgerAndItsLastRecordSeen
    } else {
      val ledgerHandle = bookKeeper.openLedgerNoRecovery(
        ledger,
        BookKeeper.DigestType.MAC,
        Dice.DICE_PASSWORD
      )

      val isLedgerCompleted = bookKeeper.isClosed(ledger)
      val nextRecord = ledgerHandle.getLastAddConfirmed + 1
      
      var updatedLastLedgerAndItsLastRecordSeen = lastLedgerAndItsLastRecordSeen
      if (recordToReadToStartWith <= ledgerHandle.getLastAddConfirmed) {
        val entries = ledgerHandle.readEntries(
          recordToReadToStartWith,
          ledgerHandle.getLastAddConfirmed
        )

        while (entries.hasMoreElements) {
          val entry = entries.nextElement()
          val entryData = entry.getEntry
          println(
            s"Ledger = ${ledgerHandle.getId}, " +
              s"RecordID = ${entry.getEntryId}, " +
              s"Value = ${bytesToIntsArray(entryData).head}, " +
              "following"
          )
          updatedLastLedgerAndItsLastRecordSeen = EntryId(ledger, entry.getEntryId)
        }
      }
      Thread.sleep(1000)
      monitorLedgerUntilItIsCompleted(
        ledger,
        updatedLastLedgerAndItsLastRecordSeen,
        nextRecord,
        isLedgerCompleted
      )
    }
  }



  private final def readUntilWeAreSlave(ledgers: Array[Long],
                                  lastLedgerAndItsLastRecordSeen: EntryId
                                 ): EntryId = {
    ledgers.foldRight(lastLedgerAndItsLastRecordSeen)((ledger, lastLedgerAndItsLastRecordSeen) =>
      monitorLedgerUntilItIsCompleted(ledger,
        lastLedgerAndItsLastRecordSeen,
        0L,
        isLedgerCompleted = false
      )
    )
  }

  @tailrec
  private final def retrieveUpcomingLedgers(path: String, ledgers: Array[Long], lastReadEntry: EntryId): EntryId = {
    if (!leaderSelector.hasLeadership) {
      val lastLedgerAndItsLastRecordSeen =
        readUntilWeAreSlave(ledgers, lastReadEntry)

      val ledgersIDsBinary = client.getData
        .forPath(path)

      val newLedgers = bytesToLongsArray(ledgersIDsBinary)
      val upcomingLedgers = newLedgers.takeRight(
        newLedgers.indexOf(lastLedgerAndItsLastRecordSeen.ledgerId + 1)
      )

      retrieveUpcomingLedgers(
        path,
        upcomingLedgers,
        lastLedgerAndItsLastRecordSeen
      )
    } else {
      lastReadEntry
    }
  }

  def follow(skipPast: EntryId): EntryId = {
    val ledgers =
      retrieveLedgersUntilNodeDoesntExist(DICE_LOG, skipPast)
    val lastLedgerAndItsLastRecordSeen =
      retrieveUpcomingLedgers(DICE_LOG, ledgers,  skipPast)

    lastLedgerAndItsLastRecordSeen
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
//    leaderSelector.close()
    client.close()
  }
}

private object Dice{
  val noLeadgerId = -1

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
