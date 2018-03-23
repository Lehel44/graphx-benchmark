package model

case class OrganisationProperty(
                                 val organisationType: Option[String],
                                 val name: Option[String],
                                 val url: Option[String],
                                 val place: Option[Long]
                               ) extends VertexProperty
