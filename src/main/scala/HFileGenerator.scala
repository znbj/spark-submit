import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SparkSession}

/** 仅生成 HFile，不执行 BulkLoad 导入。
  *
  * 适用于"生成"与"导入"需要分离的场景，例如：
  *   - 先离线生成 HFile，由运维人员在维护窗口执行导入
  *   - 多批次生成 HFile 后统一导入，减少对线上 HBase 的冲击
  *
  * 生成的 HFile 路径由调用方指定或自动生成，方法返回实际写出路径。
  * 后续可通过 BulkLoadHFiles.create(conf).bulkLoad(table, path) 完成导入。
  *
  * ZooKeeper 配置同 HbaseBulkLoadUtil，三级解析：
  *   显式参数 zkQuorum > spark.hadoop.hbase.zookeeper.quorum > classpath hbase-site.xml
  */
object HFileGenerator {

  private val OUTPUT_TABLE_NAME_CONF_KEY =
    "hbase.mapreduce.hfileoutputformat.table.name"

  /** 从 DataFrame 生成 HFile 到 HDFS，返回实际写出路径。
    *
    * @param spark        SparkSession
    * @param df           源数据，必须包含 rowKeyCol 列
    * @param tableNameStr HBase 表名，格式 "namespace:table" 或 "table"
    * @param columnFamily 列族名
    * @param rowKeyCol    作为 RowKey 的列名
    * @param outputPath   HFile 输出路径；为 null 时自动生成到 userBase/hfile_output/<table>_<ts>
    * @param userBase     用户 HDFS 根目录，用于 staging 和自动输出路径
    * @param timestamp    写入 KeyValue 的时间戳，默认当前毫秒
    * @param zkQuorum     ZooKeeper 地址，多个以逗号分隔
    * @param zkPort       ZooKeeper 端口，默认 2181
    * @param zkZnodeParent HBase 在 ZooKeeper 的根节点，默认 /hbase
    * @return 实际写出的 HFile HDFS 路径
    */
  def generate(
      spark: SparkSession,
      df: DataFrame,
      tableNameStr: String,
      columnFamily: String,
      rowKeyCol: String,
      outputPath: String = null,
      userBase: String = "/user/aiip_001",
      timestamp: Long = System.currentTimeMillis(),
      zkQuorum: String = null,
      zkPort: String = "2181",
      zkZnodeParent: String = "/hbase"
  ): String = {

    val ts = timestamp
    val safeTableName = tableNameStr.replace(":", "_")
    val hfilePath = Option(outputPath).filter(_.nonEmpty)
      .getOrElse(s"$userBase/hfile_output/${safeTableName}_$ts")

    // ========================================
    // 1. HBase 配置
    // ========================================
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    val hbaseConf = HBaseConfiguration.create(sparkHadoopConf)

    // ZooKeeper 地址解析：显式参数 > spark.hadoop.* > classpath hbase-site.xml
    val resolvedZkQuorum: String = Option(zkQuorum).filter(_.nonEmpty).getOrElse {
      spark.conf
        .getOption("spark.hadoop.hbase.zookeeper.quorum")
        .filter(_.nonEmpty)
        .getOrElse {
          val fromClasspath = hbaseConf.get("hbase.zookeeper.quorum", "")
          if (fromClasspath.nonEmpty && fromClasspath != "localhost") {
            fromClasspath
          } else {
            throw new IllegalArgumentException(
              "[HFileGenerator] hbase.zookeeper.quorum 未配置。请通过以下方式提供：\n" +
                "  1. generate(..., zkQuorum = \"zk1,zk2,zk3\")\n" +
                "  2. spark-submit --conf spark.hadoop.hbase.zookeeper.quorum=zk1,zk2,zk3\n" +
                "  3. 将 hbase-site.xml 加入 --files 并配置 extraClassPath"
            )
          }
        }
    }

    hbaseConf.set("hbase.zookeeper.quorum", resolvedZkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort)
    hbaseConf.set("zookeeper.znode.parent", zkZnodeParent)
    hbaseConf.set(OUTPUT_TABLE_NAME_CONF_KEY, tableNameStr)
    setStagingDirs(hbaseConf, userBase)
    setStagingDirs(sparkHadoopConf, userBase)

    println(s"[HFileGenerator] 目标表: $tableNameStr, RowKey列: $rowKeyCol")
    println(s"[HFileGenerator] ZooKeeper: $resolvedZkQuorum:$zkPort")
    println(s"[HFileGenerator] HFile输出路径: $hfilePath")

    // ========================================
    // 2. DataFrame -> (BulkLoadSortKey, KeyValue)
    // ========================================
    val sortedColumns = df.columns.filter(_ != rowKeyCol).sorted
    val cfBytes = Bytes.toBytes(columnFamily)
    println(s"[HFileGenerator] 数据列(已排序): ${sortedColumns.mkString(", ")}")

    val hbaseRdd = df.rdd.flatMap { row =>
      val rowKeyVal = row.getAs[Any](rowKeyCol)
      if (rowKeyVal == null || rowKeyVal.toString.trim.isEmpty) {
        Iterator.empty
      } else {
        val rkBytes = Bytes.toBytes(rowKeyVal.toString)
        sortedColumns.iterator.flatMap { colName =>
          val value = row.getAs[Any](colName)
          if (value != null) {
            val qualifierBytes = Bytes.toBytes(colName)
            val kv = new KeyValue(
              rkBytes,
              cfBytes,
              qualifierBytes,
              ts,
              Bytes.toBytes(value.toString)
            )
            Iterator.single((BulkLoadSortKey(rkBytes, qualifierBytes), kv))
          } else {
            Iterator.empty
          }
        }
      }
    }

    // ========================================
    // 3. 获取 Region 分区信息 + 单次 shuffle 排序
    // ========================================
    var connection: Connection = null
    var hTable: Table = null
    var regionLocator: RegionLocator = null

    try {
      connection = ConnectionFactory.createConnection(hbaseConf)
      val targetTable = TableName.valueOf(tableNameStr)
      hTable = connection.getTable(targetTable)
      regionLocator = connection.getRegionLocator(targetTable)

      val startKeys = regionLocator.getStartKeys
      println(s"[HFileGenerator] HBase表共 ${startKeys.length} 个Region")

      val job = Job.getInstance(hbaseConf)
      HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator)

      val partitioner = new RegionPartitioner(startKeys)
      implicit val ordering: Ordering[BulkLoadSortKey] = BulkLoadSortKey.ordering

      val sortedRdd = hbaseRdd
        .repartitionAndSortWithinPartitions(partitioner)
        .map { case (sortKey, kv) =>
          (new ImmutableBytesWritable(sortKey.rowKey), kv)
        }

      // ========================================
      // 4. 写出 HFile
      // ========================================
      println("[HFileGenerator] 开始生成HFile...")
      sortedRdd.saveAsNewAPIHadoopFile(
        hfilePath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration
      )
      println(s"[HFileGenerator] HFile生成完毕: $hfilePath")

      hfilePath

    } catch {
      case e: Exception =>
        System.err.println(s"[HFileGenerator] 失败: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      closeQuietly(regionLocator)
      closeQuietly(hTable)
      closeQuietly(connection)
    }
  }

  private def setStagingDirs(conf: Configuration, base: String): Unit = {
    conf.set("mapreduce.jobtracker.staging.root.dir", s"$base/.staging/mapred")
    conf.set("yarn.app.mapreduce.am.staging-dir", s"$base/.staging/yarn")
    conf.set("hadoop.tmp.dir", s"$base/.staging/hadoop_tmp")
    conf.set("mapreduce.cluster.local.dir", s"$base/.staging/local")
    conf.set("mapreduce.job.local.dir", s"$base/.staging/job_local")
    conf.set("mapreduce.cluster.temp.dir", s"$base/.staging/cluster_tmp")
  }

  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try { closeable.close() }
      catch {
        case e: Exception =>
          System.err.println(s"[HFileGenerator] 关闭资源异常: ${e.getMessage}")
      }
    }
  }
}
