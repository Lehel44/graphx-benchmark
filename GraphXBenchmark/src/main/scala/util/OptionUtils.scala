package util

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

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

  def dfCommentSchema(columnNames: List[String]): StructType = StructType(Seq(
    StructField(name = "title", dataType = StringType, nullable = false),
    StructField(name = "creationDate", dataType = StringType, nullable = false),
    StructField(name = "moderator", dataType = LongType, nullable = false ))
  )


}
