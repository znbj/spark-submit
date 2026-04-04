import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{
  Connection,
  ConnectionFactory,
  RegionLocator,
  Table
}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
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
    // TODO: 在此添加启动逻辑
    println("HbaseBulkLoadUtil started")
    val spark = SparkSession
      .builder()
      .appName("HbaseBulkLoadUtil")
      .master("local[*]")
      .getOrCreate()

  }

  /** Spark DataFrame 批量卸数至 HBase 2.4.5
    *
    * 优化点:
    *   1. 单次 shuffle：用 Region 感知分区器 + repartitionAndSortWithinPartitions 替代
    *      repartition + sortBy 双 shuffle
    *   2. 资源安全关闭：hTable / regionLocator / connection 全部在 finally 中释放
    *   3. 递归赋权修复：子目录 + 文件统一处理，不再遗漏中间目录
    *   4. 临时目录统一在 /user/aiip_001 下，匹配集群权限分配
    */
  def bulkLoad(
      spark: SparkSession,
      df: DataFrame,
      tableNameStr: String,
      columnFamily: String,
      rowKeyCol: String
  ): Unit = {

    // 所有临时目录统一在 /user/aiip_001 下，匹配集群已授权路径
    val userBase = "/user/aiip_001"
    val ts = System.currentTimeMillis()
    val safeTableName = tableNameStr.replace(":", "_")
    val hdfsTempPath = s"$userBase/bulkload_tmp/${safeTableName}_$ts"

    // ========================================
    // 2. HBase / MapReduce 参数配置
    // ========================================
    val sparkHadoopConf = spark.sparkContext.hadoopConfiguration
    // 从 Spark 已生效的 Hadoop 配置派生，避免 HBase / FS / Job 使用不同集群上下文。
    val hbaseConf = HBaseConfiguration.create(sparkHadoopConf)
    hbaseConf.set(OUTPUT_TABLE_NAME_CONF_KEY, tableNameStr)
    hbaseConf.setInt("hbase.bulkload.retries.number", 100)
    // --- MapReduce / YARN staging 目录 (解决 /user/hadoop 权限问题的关键) ---
    hbaseConf.set(
      "mapreduce.jobtracker.staging.root.dir",
      s"$userBase/.staging/mapred"
    )
    hbaseConf.set(
      "yarn.app.mapreduce.am.staging-dir",
      s"$userBase/.staging/yarn"
    )
    hbaseConf.set("hadoop.tmp.dir", s"$userBase/.staging/hadoop_tmp")

    // --- HBase BulkLoad staging ---
    hbaseConf.set(
      "hbase.bulkload.staging.dir",
      s"$userBase/.staging/hbase_bulkload"
    )

    // --- 以下三项也可能隐式访问 /user/hadoop，一并拦截 ---
    hbaseConf.set("mapreduce.cluster.local.dir", s"$userBase/.staging/local")
    hbaseConf.set("mapreduce.job.local.dir", s"$userBase/.staging/job_local")
    hbaseConf.set(
      "mapreduce.cluster.temp.dir",
      s"$userBase/.staging/cluster_tmp"
    )

    // 同步写入 Spark 的 hadoopConfiguration，确保 Spark 创建的 Job 也继承这些路径
    sparkHadoopConf.set(
      "mapreduce.jobtracker.staging.root.dir",
      s"$userBase/.staging/mapred"
    )
    sparkHadoopConf.set(
      "yarn.app.mapreduce.am.staging-dir",
      s"$userBase/.staging/yarn"
    )
    sparkHadoopConf.set("hadoop.tmp.dir", s"$userBase/.staging/hadoop_tmp")
    sparkHadoopConf.set(
      "mapreduce.cluster.local.dir",
      s"$userBase/.staging/local"
    )
    sparkHadoopConf.set(
      "mapreduce.job.local.dir",
      s"$userBase/.staging/job_local"
    )
    sparkHadoopConf.set(
      "mapreduce.cluster.temp.dir",
      s"$userBase/.staging/cluster_tmp"
    )

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
        val rkBytes = Bytes.toBytes(rowKeyVal.toString)
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
      // ================================
      println("[BulkLoad] HFile生成完毕")

      // 7. HDFS 赋权（递归处理目录+文件）
      // ========================================
      // 与 HFile 写出阶段共用同一份配置，避免 chmod/delete 指向不同的 FS。
      val fs = FileSystem.get(job.getConfiguration)
      val tempPath = new Path(hdfsTempPath)
      val perm775 = new FsPermission(
        FsAction.ALL,
        FsAction.READ_EXECUTE,
        FsAction.READ_EXECUTE
      )
      chmodRecursive(fs, tempPath, perm775)
      println("[BulkLoad] HDFS目录赋权完毕")

      // ========================================
      // 8. BulkLoad 导入 HBase
      // ========================================
      println("[BulkLoad] 开始BulkLoad导入...")
      val bulkLoader = BulkLoadHFiles.create(job.getConfiguration)
      bulkLoader.bulkLoad(targetTable, tempPath)
      println(s"[BulkLoad] 导入完成! 表: $tableNameStr")

    } catch {
      case e: Exception =>
        System.err.println(s"[BulkLoad] 失败: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      // 按获取的逆序关闭，避免资源泄漏
      closeQuietly(regionLocator)
      closeQuietly(hTable)
      closeQuietly(connection)

      // 清理 HFile 临时目录
      try {
        val fs = FileSystem.get(hbaseConf)
        val tempPath = new Path(hdfsTempPath)
        if (fs.exists(tempPath)) {
          fs.delete(tempPath, true)
          println(s"[BulkLoad] 临时目录已清理: $hdfsTempPath")
        }
      } catch {
        case e: Exception =>
          System.err.println(s"[BulkLoad] 清理临时目录失败: ${e.getMessage}")
      }
    }
  }

  /** 递归赋权：先处理当前路径，再遍历子目录和文件 修复原版 listFiles 只返回文件、遗漏子目录的问题
    */
  private def chmodRecursive(
      fs: FileSystem,
      path: Path,
      perm: FsPermission
  ): Unit = {
    fs.setPermission(path, perm)
    if (fs.getFileStatus(path).isDirectory) {
      fs.listStatus(path).foreach { status =>
        chmodRecursive(fs, status.getPath, perm)
      }
    }
  }

  /** 安全关闭资源，忽略异常 */
  private def closeQuietly(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try { closeable.close() }
      catch { case _: Exception => }
    }
  }
}

/** Region 感知分区器
  *
  * 根据 HBase 表的实际 Region 起始键做二分查找， 保证同一 Region 的数据落在同一个 Spark Partition， 配合
  * repartitionAndSortWithinPartitions 实现单次 shuffle 完成分区+排序。
  */
class RegionPartitioner(splitKeys: Array[Array[Byte]]) extends Partitioner {

  override def numPartitions: Int = splitKeys.length

  override def getPartition(key: Any): Int = {
    val rowKey = key match {
      case ibw: ImmutableBytesWritable =>
        Bytes.copy(ibw.get(), ibw.getOffset, ibw.getLength)
      case sortKey: BulkLoadSortKey =>
        sortKey.rowKey
    }

    // 二分查找：找到 rowKey 所属的 Region
    var low = 1 // splitKeys(0) 是空字节数组，跳过
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

final case class BulkLoadSortKey(rowKey: Array[Byte], qualifier: Array[Byte])

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
