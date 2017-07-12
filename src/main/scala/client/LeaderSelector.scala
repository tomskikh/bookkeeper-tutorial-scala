package client

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter

class LeaderSelector(client: CuratorFramework,
                     electionPath: String)
  extends LeaderSelectorListenerAdapter
    with ServerRole {

  private val leaderSelector = {
    val leader =
      new org.apache.curator.framework.recipes.leader.LeaderSelector(
        client,
        electionPath,
        this
      )
    leader.autoRequeue()
    leader.start()

    leader
  }

  override def hasLeadership: Boolean = leaderSelector.hasLeadership

  @throws[Exception]
  override def takeLeadership(client: CuratorFramework): Unit = {
    this.synchronized {
      println("Becoming leader")
      try {
        while (true) this.wait()
      }
      catch {
        case _: InterruptedException =>
          Thread.currentThread.interrupt()
      }
    }
  }

  final def close(): Unit = leaderSelector.close()
}
