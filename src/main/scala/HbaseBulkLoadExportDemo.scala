import org.apache.spark.sql.SparkSession

object HbaseBulkLoadExportDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HbaseBulkLoadExportDemo")
      .enableHiveSupport()
      .getOrCreate()

    try {
      val zkQuorum = "zk1,zk2,zk3"
      val tableName = "default:user_info_hbase"
      val columnFamily = "cf"
      val rowKeyCol = "id"
      val outputPath = "/user/aiip_001/hfile/user_info_hbase"
      val userDir = "/user/aiip_001"

      val df = spark.sql(
        """SELECT id, name, age, city
          |FROM default.user_info
          |WHERE dt = '2026-04-08'
          |""".stripMargin
      )

      HbaseBulkLoadExport.bulkLoadHbase(
        spark = spark,
        df = df,
        zkQuorum = zkQuorum,
        tableName = tableName,
        columnFamily = columnFamily,
        rowKeyCol = rowKeyCol,
        outputPath = outputPath,
        zkPort = "2181",
        userDir = userDir
      )
    } finally {
      spark.stop()
    }
  }
}
