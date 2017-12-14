package client.master

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import client.Utils._
import client.{EntryId, LeaderElectionMember}
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{AsyncCallback, BKException, BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import tracer.Tracer

import scala.annotation.tailrec

class Master(client: CuratorFramework,
             bookKeeper: BookKeeper,
             master: LeaderElectionMember,
             ledgerLogPath: String,
             password: Array[Byte],
             ensembleNumber: Int,
             writeQuorumNumber: Int,
             ackQuorumNumber: Int,
             isSync: Boolean,
             from: Int = 1000,
             to: Int = 1020) {

  private var sentEntries = 0
  private val callbackCalls = new AtomicInteger(0)
  private val rootName = Seq(
    "bk",
    if (isSync) "sync" else "async",
    ensembleNumber,
    writeQuorumNumber,
    ackQuorumNumber,
    from,
    to).mkString("-")
  println(rootName)
  private val tracer = new Tracer("localhost:9411", "bookkeeper", rootName)
  private val addEntryTracingName = "addEntry"
  private val asyncAddEntryTracingName = "asyncAddEntry"
  val isDone = new AtomicBoolean(false)

  def lead(skipPast: EntryId): EntryId = {
    val ledgerMetadata = retrieveLedgers

    val (ledgerIDs, stat, mustCreate) = (
      ledgerMetadata.ledgers,
      ledgerMetadata.stat,
      ledgerMetadata.mustCreate
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

  private def retrieveLedgers: LedgerMetadata = {
    val zNodeMetadata: Stat = new Stat()
    scala.util.Try {
      val binaryData = client.getData
        .storingStatIn(zNodeMetadata)
        .forPath(ledgerLogPath)
      val ledgers = bytesToLongsArray(binaryData)

      ledgers
    } match {
      case scala.util.Success(ledgers) =>
        LedgerMetadata(ledgers, zNodeMetadata, mustCreate = false)
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          LedgerMetadata(Array.emptyLongArray, zNodeMetadata, mustCreate = true)
        case _ =>
          throw throwable
      }
    }
  }

  private def processNewLedgersThatHaventSeenBefore(ledgers: Array[Long],
                                                    skipPast: EntryId) = {
    if (skipPast.ledgerId != noLeadgerId) {
      val index = ledgers.indexWhere(id => id >= skipPast.ledgerId)
      ledgers.slice(index, ledgers.length)
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
      .map(_.get)
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
            entry.getEntry
            newLastDisplayedEntry = EntryId(ledgeHandle.getId, entry.getEntryId)
          }
          traverseLedgersRecords(handles, nextEntry, newLastDisplayedEntry)
        }
    }

  private def ledgerHandleToWrite(ensembleNumber: Int,
                                  writeQuorumNumber: Int,
                                  ackQuorumNumber: Int,
                                  digestType: DigestType) = {
    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      password
    )
  }


  private def createLedgersLog(ledgersIDsBinary: Array[Byte]) = {
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
                               stat: Stat) = {
    scala.util.Try(
      client.setData()
        .withVersion(stat.getVersion)
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
                                  onBeingLeaderDo: LedgerHandle => Unit) = {
    try {
      while (master.hasLeadership && !isDone.get()) {
        onBeingLeaderDo(ledgerHandle)
      }
    } finally {
      ledgerHandle.close()
    }
  }


  private class Callback(name: String, trace: Boolean) extends AsyncCallback.AddCallback {
    override def addComplete(recordID: Int, ledgerHandle: LedgerHandle, l: Long, o: scala.Any): Unit = {
      if (trace) tracer.finish(name)
      callbackCalls.incrementAndGet()
    }
  }

  private val dataSize = 100
  private val data = (1 to dataSize).map(_.toByte).toArray

  private val sync: LedgerHandle => Unit = (ledgerHandle: LedgerHandle) => {
    if (sentEntries < to) {
      val name = s"$addEntryTracingName-$sentEntries"
      if (sentEntries >= from) tracer.start(name)
      ledgerHandle.addEntry(data)
      if (sentEntries >= from) tracer.finish(name)
    }
    else if (sentEntries == to) {
      tracer.close()
      isDone.set(true)
    }
    else Thread.sleep(1000)

    sentEntries += 1
  }


  private val async: LedgerHandle => Unit = (ledgerHandle: LedgerHandle) => {
    if (sentEntries < to) {
      val name = s"$asyncAddEntryTracingName-$sentEntries"
      val cb = new Callback(name, sentEntries >= from)
      if (sentEntries >= from) tracer.start(name)
      ledgerHandle.asyncAddEntry(data, cb, data)
    } else {
      callbackCalls.get match {
        case `to` =>
          tracer.close()
          callbackCalls.incrementAndGet()
          isDone.set(true)
        case i if i > to => Thread.sleep(1000)
        case _ =>
      }
    }

    sentEntries += 1
  }


  private val onBeingLeaderDo: LedgerHandle => Unit = if (isSync) sync else async
}