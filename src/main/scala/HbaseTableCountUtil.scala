import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

object HbaseTableCountUtil {

  final case class CountConfig(
      tableName: String,
      method: String = "auto",
      zkQuorum: Option[String] = None,
      zkPort: String = "2181",
      zkZnodeParent: String = "/hbase",
      startRow: Option[String] = None,
      stopRow: Option[String] = None,
      families: Seq[String] = Seq.empty,
      scanCaching: Int = 1000
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HbaseTableCountUtil")
      .getOrCreate()

    try {
      val config = parseArgs(args)
      val count = countRows(spark, config)
      println(
        s"[HbaseTableCount] table=${config.tableName}, method=${config.method}, count=$count"
      )
    } finally {
      spark.stop()
    }
  }

  def parseArgs(args: Array[String]): CountConfig = {
    if (args.isEmpty) {
      throw new IllegalArgumentException(usage())
    }

    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(s"Arguments must be provided as --key value pairs\n${usage()}")
    }

    val kv = args.grouped(2).map {
      case Array(key, value) if key.startsWith("--") => key -> value
      case Array(key, _) =>
        throw new IllegalArgumentException(s"Unsupported argument: $key\n${usage()}")
    }.toMap

    val tableName = kv.getOrElse("--table", "")
    if (tableName.trim.isEmpty) {
      throw new IllegalArgumentException(s"--table is required\n${usage()}")
    }

    val method = kv.getOrElse("--method", "auto").trim.toLowerCase
    if (!Set("auto", "coprocessor", "spark").contains(method)) {
      throw new IllegalArgumentException(s"Unsupported --method: $method")
    }
    val scanCaching = kv.get("--scan-caching").map(_.trim.toInt).getOrElse(1000)
    if (scanCaching <= 0) {
      throw new IllegalArgumentException("--scan-caching must be > 0")
    }

    CountConfig(
      tableName = tableName.trim,
      method = method,
      zkQuorum = kv.get("--zk-quorum").map(_.trim).filter(_.nonEmpty),
      zkPort = kv.getOrElse("--zk-port", "2181").trim,
      zkZnodeParent = kv.getOrElse("--zk-znode-parent", "/hbase").trim,
      startRow = kv.get("--start-row").map(_.trim).filter(_.nonEmpty),
      stopRow = kv.get("--stop-row").map(_.trim).filter(_.nonEmpty),
      families = kv
        .get("--families")
        .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty))
        .getOrElse(Seq.empty),
      scanCaching = scanCaching
    )
  }

  def countRows(spark: SparkSession, config: CountConfig): Long = {
    val hbaseConf = buildHBaseConf(spark, config)
    val scan = buildScan(config)

    config.method match {
      case "coprocessor" =>
        countByCoprocessor(hbaseConf, config, scan)
      case "spark" =>
        countBySpark(spark, hbaseConf, config, scan)
      case _ =>
        try {
          val result = countByCoprocessor(hbaseConf, config, scan)
          println("[HbaseTableCount] coprocessor count succeeded")
          result
        } catch {
          case e: Exception =>
            System.err.println(
              s"[HbaseTableCount] coprocessor count failed, fallback to spark scan: ${e.getMessage}"
            )
            countBySpark(spark, hbaseConf, config, scan)
        }
    }
  }

  def buildScan(config: CountConfig): Scan = {
    val scan = new Scan()
    scan.setCacheBlocks(false)
    scan.setCaching(config.scanCaching)
    scan.readVersions(1)
    scan.setFilter(new FirstKeyOnlyFilter())

    config.startRow.foreach(row => scan.withStartRow(Bytes.toBytes(row)))
    config.stopRow.foreach(row => scan.withStopRow(Bytes.toBytes(row)))
    config.families.foreach(cf => scan.addFamily(Bytes.toBytes(cf)))
    scan
  }

  private def buildHBaseConf(
      spark: SparkSession,
      config: CountConfig
  ): Configuration = {
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    val hbaseConf = HBaseConfiguration.create(sparkHadoopConf)
    val resolvedZkQuorum = config.zkQuorum
      .orElse(
        spark.conf
          .getOption("spark.hadoop.hbase.zookeeper.quorum")
          .map(_.trim)
          .filter(_.nonEmpty)
      )
      .orElse(Option(hbaseConf.get("hbase.zookeeper.quorum")).map(_.trim).filter(_.nonEmpty))
      .getOrElse {
        throw new IllegalArgumentException(
          "hbase.zookeeper.quorum is required. Set --zk-quorum or spark.hadoop.hbase.zookeeper.quorum"
        )
      }

    hbaseConf.set("hbase.zookeeper.quorum", resolvedZkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", config.zkPort)
    hbaseConf.set("zookeeper.znode.parent", config.zkZnodeParent)
    hbaseConf
  }

  private def countByCoprocessor(
      hbaseConf: Configuration,
      config: CountConfig,
      scan: Scan
  ): Long = {
    var aggregationClient: AggregationClient = null
    try {
      aggregationClient = new AggregationClient(hbaseConf)
      aggregationClient.rowCount(
        TableName.valueOf(config.tableName),
        new LongColumnInterpreter(),
        scan
      )
    } finally {
      if (aggregationClient != null) {
        aggregationClient.close()
      }
    }
  }

  private def countBySpark(
      spark: SparkSession,
      hbaseConf: Configuration,
      config: CountConfig,
      scan: Scan
  ): Long = {
    val scanBase64 = TableMapReduceUtil.convertScanToString(scan)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, config.tableName)
    hbaseConf.set(TableInputFormat.SCAN, scanBase64)

    spark.sparkContext
      .newAPIHadoopRDD(
        hbaseConf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
      .count()
  }

  private def usage(): String =
    """Usage:
      |spark-submit --class HbaseTableCountUtil <jar> \
      |  --table <ns:table> \
      |  [--method auto|coprocessor|spark] \
      |  [--zk-quorum zk1,zk2,zk3] \
      |  [--zk-port 2181] \
      |  [--zk-znode-parent /hbase] \
      |  [--families cf1,cf2] \
      |  [--start-row rowKeyStart] \
      |  [--stop-row rowKeyStop] \
      |  [--scan-caching 1000]
      |
      |Notes:
      |- auto: prefer HBase coprocessor rowCount, fallback to Spark scan
      |- scan uses FirstKeyOnlyFilter + cacheBlocks=false for large tables
      |""".stripMargin
}
