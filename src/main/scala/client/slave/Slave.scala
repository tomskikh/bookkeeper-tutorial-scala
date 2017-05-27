package client.slave

import org.apache.bookkeeper.client.BookKeeper
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import client.{EntryId, ServerRole}
import client.Utils._

import scala.annotation.tailrec

class Slave(client: CuratorFramework,
            bookKeeper: BookKeeper,
            slave: ServerRole,
            ledgerLogPath: String,
            password: Array[Byte]
           )
{

  def follow(skipPast: EntryId): EntryId = {
    val ledgers =
      retrieveLedgersUntilNodeDoesntExist(skipPast)
    val lastLedgerAndItsLastRecordSeen =
      retrieveUpcomingLedgers(ledgers,  skipPast)

    lastLedgerAndItsLastRecordSeen
  }


  @tailrec
  private final def retrieveLedgersUntilNodeDoesntExist(lastLedgerAndItsLastRecordSeen: EntryId): Array[Long] =
  {
    scala.util.Try {
      val ledgerIDsBinary = client.getData
        .forPath(ledgerLogPath)

      val ledgers = bytesToLongsArray(ledgerIDsBinary)

      processNewLedgersThatHaventSeenBefore(ledgers, lastLedgerAndItsLastRecordSeen)
    } match {
      case scala.util.Success(ledgers) =>
        ledgers
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          Thread.sleep(1000)
          retrieveLedgersUntilNodeDoesntExist(lastLedgerAndItsLastRecordSeen)
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

  @tailrec
  private final def monitorLedgerUntilItIsCompleted(ledger: Long,
                                                    lastLedgerAndItsLastRecordSeen: EntryId,
                                                    recordToReadToStartWith: Long,
                                                    isLedgerCompleted: Boolean
                                                   ): EntryId = {
    if (isLedgerCompleted || slave.hasLeadership) {
      lastLedgerAndItsLastRecordSeen
    } else {
      val ledgerHandle = bookKeeper.openLedgerNoRecovery(
        ledger,
        BookKeeper.DigestType.MAC,
        password
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
  private final def retrieveUpcomingLedgers(ledgers: Array[Long], lastReadEntry: EntryId): EntryId = {
    if (!slave.hasLeadership) {
      val lastLedgerAndItsLastRecordSeen =
        readUntilWeAreSlave(ledgers, lastReadEntry)


      val ledgersIDsBinary = client.getData
        .forPath(ledgerLogPath)

      val newLedgers = bytesToLongsArray(ledgersIDsBinary)
      val upcomingLedgers = newLedgers.takeRight(
        newLedgers.indexOf(lastLedgerAndItsLastRecordSeen.ledgerId + 1)
      )

      retrieveUpcomingLedgers(
        upcomingLedgers,
        lastLedgerAndItsLastRecordSeen
      )
    } else {
      lastReadEntry
    }
  }
}
