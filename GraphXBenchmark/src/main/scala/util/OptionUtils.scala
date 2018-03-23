package util

object OptionUtils {

  def toSomeString(field: Any): Option[String] = {
    val fieldOpt = Option(field)
    fieldOpt match {
      case None => None
      case Some(value) => Some(value.toString)
    }
  }

  def toSomeLong(field: Option[String]): Option[Long] = {
    field match {
      case None => None
      case Some(value) => Some(value.toLong)
    }
  }

}
