import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.example.HbaseTableCountUtil
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Arrays, Optional}

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

    assert(config.getTableName === "default:user_info_hbase")
    assert(config.getMethod === "spark")
    assert(config.getZkQuorum.get() === "zk1,zk2,zk3")
    assert(config.getFamilies === Arrays.asList("cf1", "cf2"))
    assert(config.getStartRow.get() === "rk001")
    assert(config.getStopRow.get() === "rk999")
    assert(config.getScanCaching === 2048)
  }

  test("buildScan applies large-table count optimizations") {
    val config = new HbaseTableCountUtil.CountConfig(
      "default:user_info_hbase",
      "auto",
      Optional.empty[String](),
      "2181",
      "/hbase",
      Optional.of("rk001"),
      Optional.of("rk999"),
      Arrays.asList("cf"),
      4096
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
