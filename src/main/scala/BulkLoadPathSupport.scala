import org.apache.hadoop.fs.Path

object BulkLoadPathSupport {

  def qualifyBasePath(basePath: String, fsUri: String = null): String = {
    val normalizedBase = normalizeBasePath(basePath)
    val existingScheme = new Path(normalizedBase).toUri.getScheme

    if (existingScheme != null || isBlank(fsUri)) {
      normalizedBase
    } else {
      s"${normalizeFsUri(fsUri)}${ensureLeadingSlash(normalizedBase)}"
    }
  }

  def childPath(basePath: String, child: String): String = {
    val normalizedChild = Option(child).map(_.trim).getOrElse("")
    require(normalizedChild.nonEmpty, "child path must not be empty")

    new Path(normalizeBasePath(basePath), normalizedChild.stripPrefix("/")).toString
  }

  private def normalizeBasePath(basePath: String): String = {
    val trimmed = Option(basePath).map(_.trim).getOrElse("")
    require(trimmed.nonEmpty, "base path must not be empty")

    if (trimmed == "/") trimmed else trimmed.stripSuffix("/")
  }

  private def normalizeFsUri(fsUri: String): String = {
    val trimmed = Option(fsUri).map(_.trim).getOrElse("")
    require(trimmed.nonEmpty, "fsUri must not be empty")
    trimmed.stripSuffix("/")
  }

  private def ensureLeadingSlash(path: String): String = {
    if (path.startsWith("/")) path else s"/$path"
  }

  private def isBlank(value: String): Boolean = {
    value == null || value.trim.isEmpty
  }
}
