package kafka.admin

import joptsimple.OptionParser
import kafka.utils._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils


object AutoExpandCommand extends Logging {
  def main(args: Array[String]): Unit = {
    val opts = new AutoExpandCommandOptions(args)
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    zkUtils.getAllBrokersInCluster().foreach {
      b =>
        println(b.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).host)
    }
    println("OK")
  }
}

class AutoExpandCommandOptions(args: Array[String]) {
  val parser = new OptionParser

  val zkConnectOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the " +
    "form host:port. Multiple URLS can be given to allow fail-over.")
    .withOptionalArg()
    .defaultsTo("localhost:2181")
    .describedAs("urls")
    .ofType(classOf[String])

  val selfBroker = parser.accepts("broker", "Self broker addr")
  .withOptionalArg().defaultsTo("")

  val up_down = parser.accepts("updown", "Up or Down hook")
    .withOptionalArg()
    .defaultsTo("up")
    .ofType(classOf[String])

  val options = parser.parse(args: _*)

}