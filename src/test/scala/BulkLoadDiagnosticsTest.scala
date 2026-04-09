import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.funsuite.AnyFunSuite

class BulkLoadDiagnosticsTest extends AnyFunSuite {

  test("findRegionIndex maps row keys to the expected region") {
    val splitKeys = Array(
      Array.emptyByteArray,
      Bytes.toBytes("100"),
      Bytes.toBytes("200"),
      Bytes.toBytes("300")
    )

    assert(BulkLoadDiagnostics.findRegionIndex(splitKeys, Bytes.toBytes("001")) === 0)
    assert(BulkLoadDiagnostics.findRegionIndex(splitKeys, Bytes.toBytes("100")) === 1)
    assert(BulkLoadDiagnostics.findRegionIndex(splitKeys, Bytes.toBytes("199")) === 1)
    assert(BulkLoadDiagnostics.findRegionIndex(splitKeys, Bytes.toBytes("250")) === 2)
    assert(BulkLoadDiagnostics.findRegionIndex(splitKeys, Bytes.toBytes("999")) === 3)
  }

  test("describeExceptionChain includes wrapper and root cause in order") {
    val root = new IllegalStateException("Split occurred while grouping HFiles")
    val wrapper = new RuntimeException("bulkload failed", root)

    val summary = BulkLoadDiagnostics.describeExceptionChain(wrapper)

    assert(summary === Seq(
      "cause[0] java.lang.RuntimeException: bulkload failed",
      "cause[1] java.lang.IllegalStateException: Split occurred while grouping HFiles"
    ))
  }

  test("looksLikeRegionMovementIssue matches known bulkload retry signatures") {
    val retryError = new RuntimeException("Retry attempted 10 times without completing, bailing out")
    val notLoadedError = new RuntimeException("Bulk load aborted with 1 file(s) not yet loaded")
    val regionMovedError = new RuntimeException("NotServingRegionException: region moved while opening region")
    val genericError = new RuntimeException("permission denied")
    val falsePositiveError = new RuntimeException("region count mismatch in metrics report")

    assert(BulkLoadDiagnostics.looksLikeRegionMovementIssue(retryError))
    assert(BulkLoadDiagnostics.looksLikeRegionMovementIssue(notLoadedError))
    assert(BulkLoadDiagnostics.looksLikeRegionMovementIssue(regionMovedError))
    assert(!BulkLoadDiagnostics.looksLikeRegionMovementIssue(genericError))
    assert(!BulkLoadDiagnostics.looksLikeRegionMovementIssue(falsePositiveError))
  }
}
