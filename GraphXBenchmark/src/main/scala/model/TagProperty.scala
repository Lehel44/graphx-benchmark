package model

case class TagProperty(
                        val name: Option[String],
                        val url: Option[String],
                        val hasType: Option[Long]
                      ) extends VertexProperty
