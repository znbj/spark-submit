import org.scalatest.funsuite.AnyFunSuite

class RowKeyTransformUtilTest extends AnyFunSuite {

  test("rewriteFirstFieldWithMd5 should only rewrite the first field") {
    val rowKey = "acct001\u000320260409\u0003CNY"

    val rewritten = RowKeyTransformUtil.rewriteFirstFieldWithMd5(rowKey)

    assert(
      rewritten === s"${RowKeyTransformUtil.md5Hex("acct001")}\u000320260409\u0003CNY"
    )
  }

  test("rewriteFirstFieldWithMd5 should preserve trailing empty fields") {
    val rowKey = "acct001\u0003tail\u0003"

    val rewritten = RowKeyTransformUtil.rewriteFirstFieldWithMd5(rowKey)

    assert(
      rewritten === s"${RowKeyTransformUtil.md5Hex("acct001")}\u0003tail\u0003"
    )
  }

  test("rewriteFirstFieldWithMd5 should hash the whole key when there is no separator") {
    val rowKey = "acct001"

    val rewritten = RowKeyTransformUtil.rewriteFirstFieldWithMd5(rowKey)

    assert(rewritten === RowKeyTransformUtil.md5Hex("acct001"))
  }
}
