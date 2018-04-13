package encoders

import model.ForumProperty

object ForumConversions {
  implicit def toEncodedForum(forum: ForumProperty): (Option[String], Option[String], Option[Long]) =
    (forum.title, forum.creationDate, forum.moderator)
  implicit def toDecodedForum(tuple: (Option[String], Option[String], Option[Long])) : ForumProperty =
    ForumProperty(tuple._1, tuple._2, tuple._3)
}
