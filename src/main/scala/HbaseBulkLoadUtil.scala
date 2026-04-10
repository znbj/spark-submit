import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{
  Connection,
  ConnectionFactory,
  RegionLocator,
  Table
}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, SparkSession}

object HbaseBulkLoadUtil {

  val OUTPUT_TABLE_NAME_CONF_KEY =
    "hbase.mapreduce.hfileoutputformat.table.name"

  def main(args: Array[String]): Unit = {
    println("HbaseBulkLoadUtil started")
    val spark = SparkSession
      .builder()
      .appName("HbaseBulkLoadUtil")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql(
      """SELECT id, name, age, city
        |FROM default.user_info
        |WHERE dt = '2026-04-04'
        |""".stripMargin
    )

    bulkLoad(spark, df, "default:user_info_hbase", "cf", "id")

    spark.stop()
    println("HbaseBulkLoadUtil finished")
  }

  /** Spark DataFrame 批量卸数至 HBase 2.4.5
    *
    * 优化点:
    *   1. 单次 shuffle：用 Region 感知分区器 + repartitionAndSortWithinPartitions 替代
    *      repartition + sortBy 双 shuffle
    *   2. 资源安全关闭：hTable / regionLocator / connection 全部在 finally 中释放
    *   3. staging 目录可配置，避免隐式访问无权限路径
    *
    * ZooKeeper 配置说明：
    *   HBase ZK 地址不会从 Spark 环境自动注入，需通过以下任一方式提供：
    *   - 参数 zkQuorum 显式传入（优先级最高）
    *   - spark-submit --conf spark.hadoop.hbase.zookeeper.quorum=<hosts>
    *   - 将 hbase-site.xml 放入 spark.driver/executor.extraClassPath
    */
  def bulkLoad(
      spark: SparkSession,
      df: DataFrame,
      tableNameStr: String,
      columnFamily: String,
      rowKeyCol: String,
      userBase: String = "/user/aiip_001",
      timestamp: Long = System.currentTimeMillis(),
      bulkloadFsUri: String = null,
      zkQuorum: String = null,
      zkPort: String = "2181",
      zkZnodeParent: String = "/hbase"
  ): Unit = {

    val ts = timestamp
    val safeTableName = tableNameStr.replace(":", "_")
    val qualifiedUserBase =
      BulkLoadPathSupport.qualifyBasePath(userBase, bulkloadFsUri)
    val hdfsTempPath = BulkLoadPathSupport.childPath(
      qualifiedUserBase,
      s"bulkload_tmp/${safeTableName}_$ts"
    )

    // ========================================
    // 2. HBase / MapReduce 参数配置
    // ========================================
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    // 从 Spark 已生效的 Hadoop 配置派生，避免 HBase / FS / Job 使用不同集群上下文。
    // 注意：hbase-site.xml 不会自动加载到 sparkHadoopConf，ZK 地址需显式配置。
    val hbaseConf = HBaseConfiguration.create(sparkHadoopConf)

    // ---- ZooKeeper 地址解析（优先级：显式参数 > spark.hadoop.* > hbase-site.xml on classpath）----
    val resolvedZkQuorum: String = Option(zkQuorum).filter(_.nonEmpty).getOrElse {
      // spark-submit --conf spark.hadoop.hbase.zookeeper.quorum=xxx 会写入 spark.conf
      spark.conf
        .getOption("spark.hadoop.hbase.zookeeper.quorum")
        .filter(_.nonEmpty)
        .getOrElse {
          // 最后尝试 hbase-site.xml 是否已在 classpath 上生效
          val fromClasspath = hbaseConf.get("hbase.zookeeper.quorum", "")
          if (fromClasspath.nonEmpty && fromClasspath != "localhost") {
            fromClasspath
          } else {
            throw new IllegalArgumentException(
              "[BulkLoad] hbase.zookeeper.quorum 未配置，HBase 连接将失败。" +
                "请通过以下任一方式提供：\n" +
                "  1. bulkLoad(..., zkQuorum = \"zk1,zk2,zk3\")\n" +
                "  2. spark-submit --conf spark.hadoop.hbase.zookeeper.quorum=zk1,zk2,zk3\n" +
                "  3. 将 hbase-site.xml 加入 --files 并配置 extraClassPath"
            )
          }
        }
    }

    hbaseConf.set("hbase.zookeeper.quorum", resolvedZkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort)
    hbaseConf.set("zookeeper.znode.parent", zkZnodeParent)
    println(s"[BulkLoad] ZooKeeper: $resolvedZkQuorum:$zkPort, znodeParent: $zkZnodeParent")
    hbaseConf.set(OUTPUT_TABLE_NAME_CONF_KEY, tableNameStr)
    hbaseConf.setInt("hbase.bulkload.retries.number", 100)
    hbaseConf.set(
      "hbase.bulkload.staging.dir",
      BulkLoadPathSupport.childPath(qualifiedUserBase, ".staging/hbase_bulkload")
    )
    // 统一设置 staging 目录，解决 /user/hadoop 权限问题
    setStagingDirs(hbaseConf, qualifiedUserBase)
    setStagingDirs(sparkHadoopConf, qualifiedUserBase)
    println(
      s"[BulkLoad] staging roots: mr.am=${hbaseConf.get(\"yarn.app.mapreduce.am.staging-dir\")}, " +
        s"mr.root=${hbaseConf.get(\"mapreduce.jobtracker.staging.root.dir\")}, " +
        s"hbase.tmp=${hbaseConf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY)}"
    )
    println(s"[BulkLoad] bulkload base path: $qualifiedUserBase")

    // ========================================
    // 3. 提取列名并严格按字典序排列
    // ========================================
    val sortedColumns = df.columns.filter(_ != rowKeyCol).sorted
    val cfBytes = Bytes.toBytes(columnFamily)

    println(s"[BulkLoad] 目标表: $tableNameStr, RowKey列: $rowKeyCol")
    println(s"[BulkLoad] 数据列(已排序): ${sortedColumns.mkString(", ")}")
    println(s"[BulkLoad] HFile临时路径: $hdfsTempPath")

    // ========================================
    // 4. DataFrame -> (BulkLoadSortKey, KeyValue)
    // ========================================
    val hbaseRdd = df.rdd.flatMap { row =>
      val rowKeyVal = row.getAs[Any](rowKeyCol)
      if (rowKeyVal == null || rowKeyVal.toString.trim.isEmpty) {
        Iterator.empty
      } else {
        val rewrittenRowKey =
          RowKeyTransformUtil.rewriteFirstFieldWithMd5(rowKeyVal.toString)
        val rkBytes = Bytes.toBytes(rewrittenRowKey)
        sortedColumns.iterator.flatMap { colName =>
          val value = row.getAs[Any](colName)
          if (value != null) {
            val qualifierBytes = Bytes.toBytes(colName)
            val kv = new KeyValue(
              rkBytes,
              cfBytes,
              qualifierBytes,
              ts, // 统一时间戳，保证同批数据版本一致
              Bytes.toBytes(value.toString)
            )
            // 先携带 rowKey + qualifier 排序，确保同一行内的 Cell 顺序满足 HFile 要求。
            Iterator.single((BulkLoadSortKey(rkBytes, qualifierBytes), kv))
          } else {
            Iterator.empty
          }
        }
      }
    }

    // ========================================
    // 5. 获取 Region 分区信息 + 单次 shuffle 排序
    // ========================================
    var connection: Connection = null
    var hTable: Table = null
    var regionLocator: RegionLocator = null

    try {
      connection = ConnectionFactory.createConnection(hbaseConf)
      val targetTable = TableName.valueOf(tableNameStr)
      hTable = connection.getTable(targetTable)
      regionLocator = connection.getRegionLocator(targetTable)

      // 获取 Region 起始键用于精确分区
      val startKeys = regionLocator.getStartKeys
      println(s"[BulkLoad] HBase表共 ${startKeys.length} 个Region")
      BulkLoadDiagnostics
        .formatRegionBoundarySummary(startKeys)
        .foreach(line => println(s"[BulkLoad] $line"))

      // configureIncrementalLoad 内部会设置 MapOutputKeyClass 等，无需手动设置
      val job = Job.getInstance(hbaseConf)
      HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator)

      // 单次 shuffle: Region 感知分区 + 分区内排序
      val partitioner = new RegionPartitioner(startKeys)
      implicit val ordering: Ordering[BulkLoadSortKey] =
        BulkLoadSortKey.ordering
      val sortedRdd = hbaseRdd
        .repartitionAndSortWithinPartitions(partitioner)
        .map { case (sortKey, kv) =>
          // 排序完成后再还原成 HFileOutputFormat2 需要的 rowKey 输出键。
          (new ImmutableBytesWritable(sortKey.rowKey), kv)
        }

      // ========================================
      // 6. 写出 HFile
      // ========================================
      println("[BulkLoad] 开始生成HFile...")
      sortedRdd.saveAsNewAPIHadoopFile(
        hdfsTempPath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration
      )
      println("[BulkLoad] HFile生成完毕")

      // ========================================
      // 7. BulkLoad 导入 HBase
      // ========================================
      println("[BulkLoad] 开始BulkLoad导入...")
      val tempPath = new Path(hdfsTempPath)
      val fs = tempPath.getFileSystem(job.getConfiguration)
      val bulkLoader = BulkLoadHFiles.create(job.getConfiguration)
      bulkLoader.bulkLoad(targetTable, tempPath)
      println(s"[BulkLoad] 导入完成! 表: $tableNameStr")

      // 清理 HFile 临时目录
      if (fs.exists(tempPath)) {
        fs.delete(tempPath, true)
        println(s"[BulkLoad] 临时目录已清理: $hdfsTempPath")
      }

    } catch {
      case e: Exception =>
        System.err.println(s"[BulkLoad] 失败: ${e.getMessage}")
        BulkLoadDiagnostics
          .describeExceptionChain(e)
          .foreach(line => System.err.println(s"[BulkLoad] $line"))
        if (BulkLoadDiagnostics.looksLikeRegionMovementIssue(e)) {
          System.err.println(
            "[BulkLoad] 检测到疑似 region split/move 或 region 边界不稳定。请同时核查目标表 region 变更和生成 HFile 的 rowkey 边界。"
          )
        }
        e.printStackTrace()
        // 异常时也尝试清理临时目录
        try {
          val tempPath = new Path(hdfsTempPath)
          val fs = tempPath.getFileSystem(hbaseConf)
          if (fs.exists(tempPath)) {
            fs.delete(tempPath, true)
            println(s"[BulkLoad] 临时目录已清理: $hdfsTempPath")
          }
        } catch {
          case ce: Exception =>
            System.err.println(s"[BulkLoad] 清理临时目录失败: ${ce.getMessage}")
        }
        throw e
    } finally {
      // 按获取的逆序关闭，避免资源泄漏
      closeQuietly(regionLocator)
      closeQuietly(hTable)
      closeQuietly(connection)
    }
  }

  /** 统一设置 staging 目录，避免隐式访问 /user/hadoop */
  private def setStagingDirs(conf: Configuration, base: String): Unit = {
    conf.set(
      "mapreduce.jobtracker.staging.root.dir",
      BulkLoadPathSupport.childPath(base, ".staging/mapred")
    )
    conf.set(
      "yarn.app.mapreduce.am.staging-dir",
      BulkLoadPathSupport.childPath(base, ".staging/yarn")
    )
    conf.set(
      "hadoop.tmp.dir",
      BulkLoadPathSupport.childPath(base, ".staging/hadoop_tmp")
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

  /** 安全关闭资源，异常时打印警告 */
  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try { closeable.close() }
      catch {
        case e: Exception =>
          System.err.println(s"[BulkLoad] 关闭资源异常: ${e.getMessage}")
      }
    }
  }
}

/** Region 感知分区器
  *
  * 根据 HBase 表的实际 Region 起始键做二分查找， 保证同一 Region 的数据落在同一个 Spark Partition， 配合
  * repartitionAndSortWithinPartitions 实现单次 shuffle 完成分区+排序。
  */
class RegionPartitioner(splitKeys: Array[Array[Byte]]) extends Partitioner {

  require(splitKeys.nonEmpty, "splitKeys must not be empty")

  override def numPartitions: Int = splitKeys.length

  override def getPartition(key: Any): Int = {
    val rowKey = key match {
      case ibw: ImmutableBytesWritable =>
        Bytes.copy(ibw.get(), ibw.getOffset, ibw.getLength)
      case sortKey: BulkLoadSortKey =>
        sortKey.rowKey
    }

    // 二分查找：找到 rowKey 所属的 Region
    BulkLoadDiagnostics.findRegionIndex(splitKeys, rowKey)
  }

}

final case class BulkLoadSortKey(rowKey: Array[Byte], qualifier: Array[Byte]) {

  override def equals(obj: Any): Boolean = obj match {
    case that: BulkLoadSortKey =>
      java.util.Arrays.equals(this.rowKey, that.rowKey) &&
        java.util.Arrays.equals(this.qualifier, that.qualifier)
    case _ => false
  }

  override def hashCode(): Int = {
    31 * java.util.Arrays.hashCode(rowKey) + java.util.Arrays.hashCode(qualifier)
  }
}

object BulkLoadSortKey {
  val ordering: Ordering[BulkLoadSortKey] = (a, b) => {
    val rowCompare = Bytes.compareTo(a.rowKey, b.rowKey)
    if (rowCompare != 0) {
      rowCompare
    } else {
      // 同一 rowKey 下继续按 qualifier 排序，保证 Cell 写入顺序稳定。
      Bytes.compareTo(a.qualifier, b.qualifier)
    }
  }
}
