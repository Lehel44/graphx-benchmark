package model

case class TagClassProperty(
                             val name: Option[String],
                             val url: Option[String],
                             val isSubclassOf: Option[Long]
                           ) extends VertexProperty
