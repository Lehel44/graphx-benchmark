package model

case class ForumProperty(
                          val title: Option[String],
                          val creationDate: Option[String],
                          val moderator: Option[Long]
                        ) extends VertexProperty
