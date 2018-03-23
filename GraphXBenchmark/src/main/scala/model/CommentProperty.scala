package model

case class CommentProperty(val creationDate: Option[String],
                           val locationIP: Option[String],
                           val browserUsed: Option[String],
                           val content: Option[String],
                           val length: Option[Long],
                           val creator: Option[Long],
                           val place: Option[Long],
                           val replyOfPost: Option[Long],
                           val replyOfComment: Option[Long]
                          ) extends VertexProperty
