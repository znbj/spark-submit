import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SparkSession}

object HFileWriter {

  /**
   * 将 DataFrame 生成 HFile 写到 HDFS 指定路径。
   *
   * @param spark         SparkSession
   * @param df            源数据
   * @param zkQuorum      ZooKeeper 地址，多个以逗号分隔，如 "zk1,zk2,zk3"
   * @param tableName     HBase 表名，如 "ns:table" 或 "table"
   * @param columnFamily  列族
   * @param rowKeyCol     作为 RowKey 的列名
   * @param outputPath    HFile 输出路径，如 "/user/aiip/hfile/user_info"
   * @param zkPort        ZooKeeper 端口，默认 2181
   * @param userDir       当前用户 HDFS 根目录，用于 staging，默认 /user/aiip_001
   */
  def write(
      spark: SparkSession,
      df: DataFrame,
      zkQuorum: String,
      tableName: String,
      columnFamily: String,
      rowKeyCol: String,
      outputPath: String,
      zkPort: String = "2181",
      userDir: String = "/user/aiip_001"
  ): Unit = {

    // 1. HBase 配置
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    val conf = HBaseConfiguration.create(sparkHadoopConf)
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    // staging 目录定向到有权限的路径，避免访问 /user/hadoop。
    // 必须同时设置 conf 和 sparkHadoopConf：
    //   conf          -> Job.getInstance(conf) -> saveAsNewAPIHadoopFile 的 outputFormat 配置
    //   sparkHadoopConf -> Spark task 执行时合并进 task 配置，若不设则集群默认覆盖上面的设置
    setStagingDirs(conf, userDir)
    setStagingDirs(sparkHadoopConf, userDir)
    println(
      s"[HFileWriter] staging roots: mr.am=${conf.get(\"yarn.app.mapreduce.am.staging-dir\")}, " +
        s"mr.root=${conf.get(\"mapreduce.jobtracker.staging.root.dir\")}, " +
        s"hbase.tmp=${conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY)}"
    )

    // 2. 连接 HBase，configureIncrementalLoad 会从表的列族读取压缩、BloomFilter 等参数
    val tn = TableName.valueOf(tableName)
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(tn)
    val regionLocator = connection.getRegionLocator(tn)

    try {
      val job = Job.getInstance(conf)
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      val cf  = Bytes.toBytes(columnFamily)
      val ts  = System.currentTimeMillis()
      // 排除 rowKey 列，其余列按字典序排序（HFile 要求 qualifier 有序）
      val cols = df.columns.filter(_ != rowKeyCol).sorted

      // 3. 转换为 (ImmutableBytesWritable, KeyValue)
      val kvRdd = df.rdd.flatMap { row =>
        val rk = row.getAs[Any](rowKeyCol)
        if (rk == null || rk.toString.trim.isEmpty) {
          Iterator.empty
        } else {
          val rkBytes = Bytes.toBytes(rk.toString)
          cols.iterator.flatMap { col =>
            val v = row.getAs[Any](col)
            if (v == null) Iterator.empty
            else {
              val kv = new KeyValue(rkBytes, cf, Bytes.toBytes(col), ts, Bytes.toBytes(v.toString))
              Iterator.single((new ImmutableBytesWritable(rkBytes), kv))
            }
          }
        }
      }

      // 4. 按 rowkey、qualifier 字节序全局排序（HFileOutputFormat2 强制要求有序输入）
      // Scala 2.12 对 trait SAM 推断不稳定，用匿名类显式实现 Ordering
      implicit val byteOrd: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
        override def compare(a: Array[Byte], b: Array[Byte]): Int = Bytes.compareTo(a, b)
      }
      // HBase 2.x KeyValue 无 getQualifier()，用 CellUtil.cloneQualifier
      val sorted = kvRdd.sortBy { case (k, kv) => (k.get(), CellUtil.cloneQualifier(kv)) }

      // 5. 写出 HFile
      sorted.saveAsNewAPIHadoopFile(
        outputPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration
      )

      println(s"HFile 生成完成: $outputPath")

    } finally {
      regionLocator.close()
      table.close()
      connection.close()
    }
  }

  private def setStagingDirs(conf: org.apache.hadoop.conf.Configuration, base: String): Unit = {
    conf.set("yarn.app.mapreduce.am.staging-dir", s"$base/.staging/yarn")
    conf.set("mapreduce.jobtracker.staging.root.dir", s"$base/.staging/mapred")
    conf.set("hadoop.tmp.dir", s"$base/.staging/tmp")
    conf.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, s"$base/.staging/hbase_tmp")
    conf.set("mapreduce.cluster.local.dir", s"$base/.staging/local")
    conf.set("mapreduce.job.local.dir", s"$base/.staging/job_local")
    conf.set("mapreduce.cluster.temp.dir", s"$base/.staging/cluster_tmp")
  }
}
