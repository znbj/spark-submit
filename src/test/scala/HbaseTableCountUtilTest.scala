import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.scalatest.funsuite.AnyFunSuite

class HbaseTableCountUtilTest extends AnyFunSuite {

  test("parseArgs requires table name") {
    val error = intercept[IllegalArgumentException] {
      HbaseTableCountUtil.parseArgs(Array.empty)
    }
    assert(error.getMessage.contains("--table"))
  }

  test("parseArgs rejects incomplete key value pairs") {
    val error = intercept[IllegalArgumentException] {
      HbaseTableCountUtil.parseArgs(Array("--table", "default:t1", "--method"))
    }
    assert(error.getMessage.contains("--key value pairs"))
  }

  test("parseArgs resolves optional fields and count mode") {
    val config = HbaseTableCountUtil.parseArgs(
      Array(
        "--table", "default:user_info_hbase",
        "--method", "spark",
        "--zk-quorum", "zk1,zk2,zk3",
        "--families", "cf1,cf2",
        "--start-row", "rk001",
        "--stop-row", "rk999",
        "--scan-caching", "2048"
      )
    )

    assert(config.tableName === "default:user_info_hbase")
    assert(config.method === "spark")
    assert(config.zkQuorum.contains("zk1,zk2,zk3"))
    assert(config.families === Seq("cf1", "cf2"))
    assert(config.startRow.contains("rk001"))
    assert(config.stopRow.contains("rk999"))
    assert(config.scanCaching === 2048)
  }

  test("buildScan applies large-table count optimizations") {
    val config = HbaseTableCountUtil.CountConfig(
      tableName = "default:user_info_hbase",
      startRow = Some("rk001"),
      stopRow = Some("rk999"),
      families = Seq("cf"),
      scanCaching = 4096
    )

    val scan = HbaseTableCountUtil.buildScan(config)

    assert(scan.getStartRow.sameElements("rk001".getBytes("UTF-8")))
    assert(scan.getStopRow.sameElements("rk999".getBytes("UTF-8")))
    assert(scan.getCaching === 4096)
    assert(!scan.getCacheBlocks)
    assert(scan.getMaxVersions === 1)
    assert(scan.getFilter.isInstanceOf[FirstKeyOnlyFilter])
    assert(scan.getFamilies.length === 1)
  }
}
