import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

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

  def describePathConfiguration(pathOrUri: String, conf: Configuration): Seq[String] = {
    val path = new Path(normalizeBasePath(pathOrUri))
    val scheme = Option(path.toUri.getScheme).getOrElse("<default>")
    val authority = Option(path.toUri.getAuthority).filter(_.nonEmpty).getOrElse("<none>")
    val defaultFs = Option(conf.get("fs.defaultFS")).filter(_.nonEmpty).getOrElse("<unset>")
    val schemeImpl =
      Option(path.toUri.getScheme)
        .flatMap(s => Option(conf.get(s"fs.$s.impl")).filter(_.nonEmpty))
        .getOrElse("<unset>")
    val base = Seq(
      s"path=$path scheme=$scheme authority=$authority",
      s"fs.defaultFS=$defaultFs",
      s"fs.$scheme.impl=$schemeImpl"
    )

    viewFsAuthority(pathOrUri) match {
      case Some(authorityName) =>
        val mountKeys = findViewFsMountTableKeys(conf, authorityName)
        if (mountKeys.nonEmpty) {
          base :+ s"viewfs mount-table keys: ${summarizeKeys(mountKeys)}"
        } else {
          base :+ s"missing viewfs mount-table config under fs.viewfs.mounttable.$authorityName.*"
        }
      case None =>
        base
    }
  }

  def requireResolvable(pathOrUri: String, conf: Configuration): Unit = {
    val normalizedPath = normalizeBasePath(pathOrUri)
    viewFsAuthority(pathOrUri).foreach { authority =>
      val mountKeys = findViewFsMountTableKeys(conf, authority)
      if (mountKeys.isEmpty) {
        throw new IllegalArgumentException(
          s"[BulkLoad] path $normalizedPath uses viewfs authority '$authority' but the active Hadoop configuration " +
            s"has no keys under fs.viewfs.mounttable.$authority.*. " +
            "Pass the matching core-site.xml to driver/executors or provide the mount-table settings with spark-submit --conf."
        )
      }
    }

    val path = new Path(normalizedPath)
    val expectedScheme = Option(path.toUri.getScheme).filter(_.nonEmpty)

    try {
      val resolvedFs = path.getFileSystem(conf)
      expectedScheme.foreach { scheme =>
        val resolvedScheme = Option(resolvedFs.getUri.getScheme).filter(_.nonEmpty).getOrElse("<unknown>")
        if (!resolvedScheme.equalsIgnoreCase(scheme)) {
          throw new IllegalArgumentException(
            s"[BulkLoad] path $normalizedPath expected Hadoop filesystem scheme '$scheme' " +
              s"but resolved to '$resolvedScheme' (${resolvedFs.getClass.getName}). " +
              s"Check fs.$scheme.impl and the matching core-site.xml / spark-submit --conf entries."
          )
        }
      }
    } catch {
      case e: IllegalArgumentException =>
        throw e
      case NonFatal(e) =>
        val detail = Option(e.getMessage).filter(_.nonEmpty).getOrElse("<no message>")
        throw new IllegalArgumentException(
          s"[BulkLoad] path $normalizedPath could not be resolved by Hadoop FileSystem " +
            s"(${e.getClass.getName}: $detail). " +
            "Check fs.defaultFS, fs.<scheme>.impl, and the matching core-site.xml / spark-submit --conf entries.",
          e
        )
    }
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

  private def viewFsAuthority(pathOrUri: String): Option[String] = {
    val authority = Option(new Path(normalizeBasePath(pathOrUri)).toUri.getAuthority)
      .map(_.trim)
      .filter(_.nonEmpty)
    val scheme = Option(new Path(normalizeBasePath(pathOrUri)).toUri.getScheme)
      .map(_.trim)
      .filter(_.nonEmpty)

    scheme.filter(_.equalsIgnoreCase("viewfs")).flatMap(_ => authority)
  }

  private def findViewFsMountTableKeys(conf: Configuration, authority: String): Seq[String] = {
    val prefix = s"fs.viewfs.mounttable.$authority."
    conf.iterator().asScala
      .map(_.getKey)
      .filter(_.startsWith(prefix))
      .toSeq
      .sorted
  }

  private def summarizeKeys(keys: Seq[String], limit: Int = 4): String = {
    val shown = keys.take(limit)
    if (keys.length <= limit) shown.mkString(", ")
    else s"${shown.mkString(", ")}, ... (${keys.length - limit} more)"
  }
}
