import org.apache.hadoop.hbase.util.Bytes

object BulkLoadDiagnostics {

  private val RegionMovementSignals: Seq[String] = Seq(
    "split occurred while grouping hfiles",
    "retry attempted",
    "not yet loaded",
    "not online",
    "failedsanitycheck",
    "not serving region",
    "opening region",
    "closing region",
    "moving region",
    "region moved"
  )

  def findRegionIndex(splitKeys: Array[Array[Byte]], rowKey: Array[Byte]): Int = {
    require(splitKeys.nonEmpty, "splitKeys must not be empty")

    var low = 1
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

  def describeExceptionChain(error: Throwable): Seq[String] = {
    Iterator
      .iterate(Option(error))(_.flatMap(t => Option(t.getCause)))
      .takeWhile(_.nonEmpty)
      .flatten
      .zipWithIndex
      .map { case (t, idx) =>
        val message = Option(t.getMessage).filter(_.nonEmpty).getOrElse("<no message>")
        s"cause[$idx] ${t.getClass.getName}: $message"
      }
      .toSeq
  }

  def looksLikeRegionMovementIssue(error: Throwable): Boolean = {
    val haystack = describeExceptionChain(error).mkString("\n").toLowerCase(java.util.Locale.ROOT)
    RegionMovementSignals.exists(haystack.contains)
  }

  def formatRegionBoundarySummary(startKeys: Array[Array[Byte]], limit: Int = 8): Seq[String] = {
    startKeys.zipWithIndex.take(limit).map { case (key, idx) =>
      s"region[$idx] startKey=${printableKey(key)}"
    } ++ {
      if (startKeys.length > limit) Seq(s"... ${startKeys.length - limit} more region start keys omitted")
      else Seq.empty
    }
  }

  private def printableKey(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.isEmpty) "<EMPTY>"
    else {
      val text = Bytes.toStringBinary(bytes)
      if (text.length <= 64) text else s"${text.take(64)}..."
    }
  }
}
