package client.master

import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import client.{EntryId, ServerRole}
import client.Utils._

import scala.annotation.tailrec

class Master(client: CuratorFramework,
             bookKeeper: BookKeeper,
             master: ServerRole,
             ledgerLogPath: String,
             password: Array[Byte]
            )
{
  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  @volatile private var lastRecoveredLedger: Option[Long] = None
  private def recoverLedger(ledgerID: Long): Unit = {
    lastRecoveredLedger match {
      case None =>
        lastRecoveredLedger = Some(ledgerID)
      case Some(lastRecoveredLedgerID) if lastRecoveredLedgerID != ledgerID =>
        lastRecoveredLedger = Some(ledgerID)
      case _ =>
    }
  }

  def lead(skipPast: EntryId): EntryId = {
    val ledgersWithMetadataInformation =
      retrieveAllLedgersFromZkServer

    val (ledgerIDs, stat, mustCreate) = (
      ledgersWithMetadataInformation.ledgers,
      ledgersWithMetadataInformation.zNodeMetadata,
      ledgersWithMetadataInformation.mustCreate
    )

    val newLedgers: Stream[Long] =
      processNewLedgersThatHaventSeenBefore(ledgerIDs, skipPast)
        .toStream

    val newLedgerHandles: List[LedgerHandle] =
      openLedgersHandlers(newLedgers, BookKeeper.DigestType.MAC)
        .toList

    val lastDisplayedEntry: EntryId =
      traverseLedgersRecords(
        newLedgerHandles,
        EntryId(skipPast.ledgerId, skipPast.entryId + 1),
        skipPast
      )

    val ledgerHandle = ledgerHandleToWrite(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      BookKeeper.DigestType.MAC
    )

    val ledgersIDsToBytes = longArrayToBytes(ledgerIDs :+ ledgerHandle.getId)
    if (mustCreate) {
      createLedgersLog(ledgersIDsToBytes)
    } else {
      updateLedgersLog(ledgersIDsToBytes, stat)
    }

    whileLeaderDo(ledgerHandle, onBeingLeaderDo)

    lastDisplayedEntry
  }

  private def retrieveAllLedgersFromZkServer: LedgersWithMetadataInformation = {
    val zNodeMetadata: Stat = new Stat()
    scala.util.Try {
      val binaryData = client.getData
        .storingStatIn(zNodeMetadata)
        .forPath(ledgerLogPath)
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
    if (skipPast.ledgerId != noLeadgerId) {
      ledgers.filter{id => id >= skipPast.ledgerId}
    }
    else
      ledgers
  }


  private def openLedgersHandlers(ledgers: Stream[Long],
                                  digestType: DigestType
                                 ) = {
    ledgers
      .map(ledgerID =>
        scala.util.Try(
          bookKeeper.openLedger(
            ledgerID,
            digestType,
            password
          )))
      .takeWhile {
        case scala.util.Success(_) =>
          true
        case scala.util.Failure(throwable) => throwable match {
          case _: BKException.BKLedgerRecoveryException =>
            false
          case _: Throwable =>
            throw throwable
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
                                  digestType: DigestType
                                 ) = {
    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      password
    )
  }


  private def createLedgersLog(ledgersIDsBinary: Array[Byte]) =
  {
    scala.util.Try(
      client.create.forPath(ledgerLogPath, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NodeExistsException =>
        case _ => throw throwable
      }
    }
  }

  private def updateLedgersLog(ledgersIDsBinary: Array[Byte],
                               zNodeMetadata: Stat) =
  {
    scala.util.Try(
      client.setData()
        .withVersion(zNodeMetadata.getVersion)
        .forPath(ledgerLogPath, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.BadVersionException =>
        case _ =>
          throw throwable
      }
    }
  }


  private final def whileLeaderDo(ledgerHandle: LedgerHandle,
                                  onBeingLeaderDo: LedgerHandle => Unit
                                 ) = {
    try {
      while (master.hasLeadership) {
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
        s"isLeader = ${master.hasLeadership}"
    )
  }
}