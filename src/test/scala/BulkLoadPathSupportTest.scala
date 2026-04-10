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
}
