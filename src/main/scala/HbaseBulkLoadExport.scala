import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, SparkSession}

object HbaseBulkLoadExport {

  private type BulkLoadKey = (Array[Byte], Array[Byte])

  private object BulkLoadKeyOrdering extends Ordering[BulkLoadKey] with Serializable {
    override def compare(a: BulkLoadKey, b: BulkLoadKey): Int = {
      val rowCompare = Bytes.compareTo(a._1, b._1)
      if (rowCompare != 0) rowCompare
      else Bytes.compareTo(a._2, b._2)
    }
  }

  private final class RegionStartKeyPartitioner(splitKeys: Array[Array[Byte]])
      extends Partitioner
      with Serializable {
    require(splitKeys.nonEmpty, "splitKeys must not be empty")

    override def numPartitions: Int = splitKeys.length

    override def getPartition(key: Any): Int = {
      val rowKey = key.asInstanceOf[BulkLoadKey]._1
      var low = 1
      var high = splitKeys.length - 1
      var region = 0

      while (low <= high) {
        val mid = (low + high) >>> 1
        val cmp = Bytes.compareTo(rowKey, splitKeys(mid))
        if (cmp >= 0) {
          region = mid
          low = mid + 1
        } else {
          high = mid - 1
        }
      }
      region
    }
  }

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
  def bulkLoadHbase(
      spark: SparkSession,
      df: DataFrame,
      zkQuorum: String,
      tableName: String,
      columnFamily: String,
      rowKeyCol: String,
      outputPath: String,
      zkPort: String = "2181",
      userDir: String = "/user/aiip_001",
      bulkloadFsUri: String = null
  ): Unit = {

    // 1. HBase 配置
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    val conf = HBaseConfiguration.create(sparkHadoopConf)
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    val qualifiedUserDir =
      BulkLoadPathSupport.qualifyBasePath(userDir, bulkloadFsUri)
    val qualifiedOutputPath =
      BulkLoadPathSupport.qualifyBasePath(outputPath, bulkloadFsUri)
    // staging 目录定向到有权限的路径，避免访问 /user/hadoop。
    // 必须同时设置 conf 和 sparkHadoopConf：
    //   conf          -> Job.getInstance(conf) -> saveAsNewAPIHadoopFile 的 outputFormat 配置
    //   sparkHadoopConf -> Spark task 执行时合并进 task 配置，若不设则集群默认覆盖上面的设置
    setStagingDirs(conf, qualifiedUserDir)
    setStagingDirs(sparkHadoopConf, qualifiedUserDir)
    BulkLoadPathSupport
      .describePathConfiguration(qualifiedUserDir, conf)
      .foreach(line => println(s"[HbaseBulkLoadExport] $line"))
    BulkLoadPathSupport
      .describePathConfiguration(qualifiedOutputPath, conf)
      .foreach(line => println(s"[HbaseBulkLoadExport] output $line"))
    BulkLoadPathSupport.requireResolvable(qualifiedUserDir, conf)
    BulkLoadPathSupport.requireResolvable(qualifiedOutputPath, conf)
    println(
      s"[HbaseBulkLoadExport] staging roots: mr.am=${conf.get("yarn.app.mapreduce.am.staging-dir")}, " +
        s"mr.root=${conf.get("mapreduce.jobtracker.staging.root.dir")}, " +
        s"hbase.tmp=${conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY)}"
    )
    println(s"[HbaseBulkLoadExport] bulkload base path: $qualifiedUserDir")
    println(s"[HbaseBulkLoadExport] bulkload output path: $qualifiedOutputPath")

    // 2. 连接 HBase，configureIncrementalLoad 会从表的列族读取压缩、BloomFilter 等参数
    val tn = TableName.valueOf(tableName)
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(tn)
    val regionLocator = connection.getRegionLocator(tn)

    try {
      val job = Job.getInstance(conf)
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
      val startKeys = regionLocator.getStartKeys
      println(s"[HbaseBulkLoadExport] HBase表共 ${startKeys.length} 个Region")

      val cf  = Bytes.toBytes(columnFamily)
      val ts  = System.currentTimeMillis()
      // 排除 rowKey 列，其余列按字典序排序（HFile 要求 qualifier 有序）
      val cols = df.columns.filter(_ != rowKeyCol).sorted

      // 3. 转换为 (排序键, KeyValue)
      implicit val keyOrdering: Ordering[BulkLoadKey] = BulkLoadKeyOrdering

      val kvRdd = df.rdd.flatMap { row =>
        val rk = row.getAs[Any](rowKeyCol)
        if (rk == null || rk.toString.trim.isEmpty) {
          Iterator.empty
        } else {
          val rewrittenRowKey =
            RowKeyTransformUtil.rewriteFirstFieldWithMd5(rk.toString)
          val rkBytes = Bytes.toBytes(rewrittenRowKey)
          cols.iterator.flatMap { col =>
            val v = row.getAs[Any](col)
            if (v == null) Iterator.empty
            else {
              val qualifierBytes = Bytes.toBytes(col)
              val kv = new KeyValue(rkBytes, cf, qualifierBytes, ts, Bytes.toBytes(v.toString))
              Iterator.single((((rkBytes, qualifierBytes): BulkLoadKey), kv))
            }
          }
        }
      }

      // 4. 大数据量下避免全局 sortBy 双重开销，改为按 Region 分区并在分区内排序
      val sorted = kvRdd
        .repartitionAndSortWithinPartitions(new RegionStartKeyPartitioner(startKeys))
        .map { case (sortKey, kv) =>
          (new ImmutableBytesWritable(sortKey._1), kv)
        }

      // 5. 写出 HFile
      sorted.saveAsNewAPIHadoopFile(
        qualifiedOutputPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration
      )

      println(s"HFile 生成完成: $qualifiedOutputPath")

      // 6. 直接执行 BulkLoad 导入 HBase
      val tempPath = new Path(qualifiedOutputPath)
      println(s"[HbaseBulkLoadExport] 开始BulkLoad导入: $qualifiedOutputPath")
      val bulkLoader = BulkLoadHFiles.create(job.getConfiguration)
      bulkLoader.bulkLoad(tn, tempPath)
      println(s"[HbaseBulkLoadExport] BulkLoad导入完成: $tableName")

      val fs = tempPath.getFileSystem(job.getConfiguration)
      if (fs.exists(tempPath)) {
        fs.delete(tempPath, true)
        println(s"[HbaseBulkLoadExport] 临时HFile目录已清理: $qualifiedOutputPath")
      }

    } finally {
      regionLocator.close()
      table.close()
      connection.close()
    }
  }

  private def setStagingDirs(conf: org.apache.hadoop.conf.Configuration, base: String): Unit = {
    conf.set(
      "yarn.app.mapreduce.am.staging-dir",
      BulkLoadPathSupport.childPath(base, ".staging/yarn")
    )
    conf.set(
      "mapreduce.jobtracker.staging.root.dir",
      BulkLoadPathSupport.childPath(base, ".staging/mapred")
    )
    conf.set(
      "hadoop.tmp.dir",
      BulkLoadPathSupport.childPath(base, ".staging/tmp")
    )
    conf.set(
      HConstants.TEMPORARY_FS_DIRECTORY_KEY,
      BulkLoadPathSupport.childPath(base, ".staging/hbase_tmp")
    )
    conf.set(
      "mapreduce.cluster.local.dir",
      BulkLoadPathSupport.childPath(base, ".staging/local")
    )
    conf.set(
      "mapreduce.job.local.dir",
      BulkLoadPathSupport.childPath(base, ".staging/job_local")
    )
    conf.set(
      "mapreduce.cluster.temp.dir",
      BulkLoadPathSupport.childPath(base, ".staging/cluster_tmp")
    )
  }
}
