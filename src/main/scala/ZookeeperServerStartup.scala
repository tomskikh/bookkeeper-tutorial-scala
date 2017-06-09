import java.util
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.ACL

object ZookeeperServerStartup {
  val port = 2181
  val endpoints = s"127.0.0.1:$port"

  def main(args: Array[String]): Unit = {
    val zkServer = new TestingServer(port, true)

    val zkClient: CuratorFramework = {
      val connection = CuratorFrameworkFactory.builder()
        .connectString(endpoints)
        .sessionTimeoutMs(2000)
        .connectionTimeoutMs(5000)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build()

      connection.start()
      connection.blockUntilConnected(5000, TimeUnit.MILLISECONDS)
      connection
    }

    zkClient.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(BookieServerStartup.zkLedgersRootPath)

    zkClient.create()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(BookieServerStartup.zkBookiesAvailablePath)

    zkClient.close()

  }
}

