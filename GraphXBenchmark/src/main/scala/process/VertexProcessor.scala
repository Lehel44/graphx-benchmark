package process

import model._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import util.OptionUtils.{toSomeLong, toSomeString}

object VertexProcessor {

  def createCommentVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val creationDate = toSomeString(row.getAs[Any]("creationDate"))
        var locationIP = toSomeString(row.getAs[Any]("locationIP"))
        var browserUsed = toSomeString(row.getAs[Any]("browserUsed"))
        var content = toSomeString(row.getAs[Any]("content"))
        val length = toSomeLong(toSomeString(row.getAs[Any]("length")))
        val creator = toSomeLong(toSomeString(row.getAs[Any]("creator")))
        val place = toSomeLong(toSomeString(row.getAs[Any]("place")))
        val replyOfPost = toSomeLong(toSomeString(row.getAs[Any]("replyOfPost")))
        val replyOfComment = toSomeLong(toSomeString(row.getAs[Any]("replyOfComment")))
        (id, CommentProperty(creationDate, locationIP, browserUsed, content, length,
          creator, place, replyOfPost, replyOfComment))
    }
  }

  def createForumVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val title = toSomeString(row.getAs[Any]("title"))
        val creationDate = toSomeString(row.getAs[Any]("creationDate"))
        val moderator = toSomeLong(toSomeString(row.getAs[Any]("moderator")))
        (id, ForumProperty(title, creationDate, moderator))
    }
  }

  def createOrganisationVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val organisationType = toSomeString(row.getAs[Any]("type"))
        val name = toSomeString(row.getAs[Any]("name"))
        val url = toSomeString(row.getAs[Any]("url"))
        val place = toSomeLong(toSomeString(row.getAs[Any]("place")))
        (id, OrganisationProperty(organisationType, name, url, place))
    }
  }

  def createPersonVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val firstName = toSomeString(row.getAs[Any]("firstName"))
        val lastName = toSomeString(row.getAs[Any]("lastName"))
        val gender = toSomeString(row.getAs[Any]("gender"))
        val birthday = toSomeString(row.getAs[Any]("birthday"))
        val creationDate = toSomeString(row.getAs[Any]("creationDate"))
        var locationIP = toSomeString(row.getAs[Any]("locationIP"))
        var browserUsed = toSomeString(row.getAs[Any]("browserUsed"))
        val place = toSomeLong(toSomeString(row.getAs[Any]("place")))
        val language = toSomeString(row.getAs[Any]("language"))
        val email = toSomeString(row.getAs[Any]("email"))
        (id, PersonProperty(firstName, lastName, gender, birthday, creationDate,
          locationIP, browserUsed, place, language, email))
    }
  }

  def createPlaceVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val name = toSomeString(row.getAs[Any]("name"))
        val url = toSomeString(row.getAs[Any]("url"))
        val placeType = toSomeString(row.getAs[Any]("type"))
        val isPartOf = toSomeLong(toSomeString(row.getAs[Any]("place")))
        (id, PlaceProperty(name, url, placeType, isPartOf))
    }
  }

  def createPostVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        var imageFile = toSomeString(row.getAs[Any]("imageFile"))
        val creationDate = toSomeString(row.getAs[Any]("creationDate"))
        var locationIP = toSomeString(row.getAs[Any]("locationIP"))
        var browserUsed = toSomeString(row.getAs[Any]("browserUsed"))
        var language = toSomeString(row.getAs[Any]("language"))
        var content = toSomeString(row.getAs[Any]("content"))
        val length = toSomeLong(toSomeString(row.getAs[Any]("length")))
        val creator = toSomeLong(toSomeString(row.getAs[Any]("creator")))
        val forumId = toSomeLong(toSomeString(row.getAs[Any]("Forum.id")))
        val place = toSomeLong(toSomeString(row.getAs[Any]("place")))
        (id, PostProperty(imageFile, creationDate, locationIP, browserUsed,
          language, content, length, creator, forumId, place))
    }
  }

  def createTagVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val name = toSomeString(row.getAs[Any]("name"))
        val url = toSomeString(row.getAs[Any]("url"))
        val hasType = toSomeLong(toSomeString(row.getAs[Any]("hasType")))
        (id, TagProperty(name, url, hasType))
    }
  }

  def createTagClassVertexRdd(rdd: RDD[Row]): RDD[(VertexId, VertexProperty)] = {
    rdd.map {
      row =>
        val id = row.getAs[Any]("id").toString.toLong
        val name = toSomeString(row.getAs[Any]("name"))
        val url = toSomeString(row.getAs[Any]("url"))
        val isSubclassOf = toSomeLong(toSomeString(row.getAs[Any]("isSubclassOf")))
        (id, TagClassProperty(name, url, isSubclassOf))
    }
  }

}
