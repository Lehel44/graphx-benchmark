package model

sealed trait VertexProperty

final case class CommentProperty(val creationDate: Option[String],
                                 val locationIP: Option[String],
                                 val browserUsed: Option[String],
                                 val content: Option[String],
                                 val length: Option[Long],
                                 val creator: Option[Long],
                                 val place: Option[Long],
                                 val replyOfPost: Option[Long],
                                 val replyOfComment: Option[Long]
                                ) extends VertexProperty

final case class ForumProperty(
                          val title: Option[String],
                          val creationDate: Option[String],
                          val moderator: Option[Long]
                        ) extends VertexProperty

final case class OrganisationProperty(
                                 val organisationType: Option[String],
                                 val name: Option[String],
                                 val url: Option[String],
                                 val place: Option[Long]
                               ) extends VertexProperty

final case class PersonProperty(
                           val firstName: Option[String],
                           val lastName: Option[String],
                           val gender: Option[String],
                           val birthday: Option[String],
                           val creationDate: Option[String],
                           val locationIP: Option[String],
                           val browserUsed: Option[String],
                           val place: Option[Long],
                           val language: Option[String],
                           val email: Option[String]
                         ) extends VertexProperty

final case class PlaceProperty(
                          val name: Option[String],
                          val url: Option[String],
                          val placeType: Option[String],
                          val isPartOf: Option[Long]
                        ) extends VertexProperty

final case class PostProperty(
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

final case class TagClassProperty(
                             val name: Option[String],
                             val url: Option[String],
                             val isSubclassOf: Option[Long]
                           ) extends VertexProperty

final case class TagProperty(
                        val name: Option[String],
                        val url: Option[String],
                        val hasType: Option[Long]
                      ) extends VertexProperty