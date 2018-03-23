package model

case class PostProperty(
                         val imageFile: Option[String],
                         val creationDate: Option[String],
                         val locationIP: Option[String],
                         val browserUsed: Option[String],
                         val language: Option[String],
                         val content: Option[String],
                         val length: Option[Long],
                         val creator: Option[Long],
                         val forumId: Option[Long],
                         val place: Option[Long]
                       ) extends VertexProperty
