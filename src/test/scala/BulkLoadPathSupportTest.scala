import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class BulkLoadPathSupportTest extends AnyFunSuite {

  test("qualifyBasePath keeps explicit uri unchanged") {
    val path = "viewfs://nsfed/user/aiip_001"

    assert(BulkLoadPathSupport.qualifyBasePath(path, "hdfs://ignored") === path)
  }

  test("qualifyBasePath prepends remote fs uri to plain absolute path") {
    val qualified =
      BulkLoadPathSupport.qualifyBasePath("/user/aiip_001", "viewfs://nsfed")

    assert(qualified === "viewfs://nsfed/user/aiip_001")
  }

  test("qualifyBasePath prepends remote fs uri to plain relative path") {
    val qualified =
      BulkLoadPathSupport.qualifyBasePath("user/aiip_001", "viewfs://nsfed")

    assert(qualified === "viewfs://nsfed/user/aiip_001")
  }

  test("childPath appends child segment without losing uri authority") {
    val child = BulkLoadPathSupport.childPath(
      "viewfs://nsfed/user/aiip_001",
      ".staging/hbase_bulkload"
    )

    assert(child === "viewfs://nsfed/user/aiip_001/.staging/hbase_bulkload")
  }

  test("requireResolvable rejects viewfs path when mount table config is absent") {
    val conf = new Configuration(false)

    val error = intercept[IllegalArgumentException] {
      BulkLoadPathSupport.requireResolvable("viewfs://nsfed/user/aiip_001", conf)
    }

    assert(error.getMessage.contains("viewfs authority 'nsfed'"))
    assert(error.getMessage.contains("fs.viewfs.mounttable.nsfed.*"))
  }

  test("describePathConfiguration reports detected mount table keys") {
    val conf = new Configuration(false)
    conf.set("fs.defaultFS", "hdfs://nameservice1")
    conf.set("fs.viewfs.impl", "org.apache.hadoop.fs.viewfs.ViewFileSystem")
    conf.set("fs.viewfs.mounttable.nsfed.link./user", "hdfs://nameservice1/user")

    val summary =
      BulkLoadPathSupport.describePathConfiguration("viewfs://nsfed/user/aiip_001", conf)

    assert(summary.exists(_.contains("scheme=viewfs authority=nsfed")))
    assert(summary.exists(_.contains("fs.defaultFS=hdfs://nameservice1")))
    assert(summary.exists(_.contains("fs.viewfs.impl=org.apache.hadoop.fs.viewfs.ViewFileSystem")))
    assert(summary.exists(_.contains("fs.viewfs.mounttable.nsfed.link./user")))
  }
}
