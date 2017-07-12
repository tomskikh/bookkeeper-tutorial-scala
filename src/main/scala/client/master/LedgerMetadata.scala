package client.master

import org.apache.zookeeper.data.Stat

/**
  *
  * @param ledgers list of ledgers stored in the log znode
  * @param stat stat of log znode containing a version. Each time a znode is updated, a new version is assigned.
  *             This can be used for check-and-set operations, which is important to avoid race conditions in distributed systems.
  * @param mustCreate true if the zookeeper znode in which we want to store the log should be created, false otherwise
  */
case class LedgerMetadata(ledgers: Array[Long],
                          stat: Stat,
                          mustCreate: Boolean)

