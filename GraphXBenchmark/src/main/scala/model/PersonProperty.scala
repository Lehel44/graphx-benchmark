package model

case class PersonProperty(
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
