import org.apache.curator.test.TestingServer

object ZookeeperServerStartup extends App{
  lazy final val port = 2181
  val zkServer = new TestingServer(port, true)

  println(zkServer.getConnectString)
}

