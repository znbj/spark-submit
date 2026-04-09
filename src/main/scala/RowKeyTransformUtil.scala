import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.regex.Pattern

object RowKeyTransformUtil {
  private val RowKeySep = "\u0003"
  private val RowKeySepPattern = Pattern.quote(RowKeySep)

  def rewriteFirstFieldWithMd5(rowKey: String): String = {
    val parts = rowKey.split(RowKeySepPattern, -1)
    if (parts.isEmpty) {
      rowKey
    } else {
      parts(0) = md5Hex(parts(0))
      parts.mkString(RowKeySep)
    }
  }

  def md5Hex(value: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map("%02x".format(_))
      .mkString
  }
}
