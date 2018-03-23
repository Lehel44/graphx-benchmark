package model

case class PlaceProperty(
                          val name: Option[String],
                          val url: Option[String],
                          val placeType: Option[String],
                          val isPartOf: Option[Long]
                        ) extends VertexProperty
