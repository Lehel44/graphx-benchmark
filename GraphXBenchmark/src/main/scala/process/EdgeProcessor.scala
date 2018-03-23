package process

import model.edge._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object EdgeProcessor {

  def createCommentHasTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Comment.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, HasTagEdge())
    }
  }

  def createForumHasMemberPersonEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Forum.id").toString.toLong, row.getAs[Any]("Person.id").toString.toLong,
          HasMemberEdge(row.getAs[Any]("joinDate").toString))
    }
  }

  def createForumHasTagTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Forum.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, HasTagEdge())
    }
  }

  def createPersonHasInterestTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, HasInterestEdge())
    }
  }

  def createPersonKnowsPersonEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.get(0).toString.toLong, row.get(1).toString.toLong,
          KnowsEdge(row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonLikesCommentEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Comment.id").toString.toLong,
          LikesEdge(row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonLikesPostEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Post.id").toString.toLong,
          LikesEdge(row.getAs[Any]("creationDate").toString))
    }
  }

  def createPersonStudyAtOrganisationEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Organisation.id").toString.toLong,
          StudyAtEdge(row.getAs[Any]("classYear").toString.toLong))
    }
  }

  def createPersonWorkAtOrganisationEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Person.id").toString.toLong, row.getAs[Any]("Organisation.id").toString.toLong,
          WorksAtEdge(row.getAs[Any]("workFrom").toString.toLong))
    }
  }

  def createPostHasTagTagEdgeRdd(rdd: RDD[Row]): RDD[Edge[EdgeProperty]] = {
    rdd.map {
      row =>
        Edge(row.getAs[Any]("Post.id").toString.toLong, row.getAs[Any]("Tag.id").toString.toLong, HasTagEdge())
    }
  }

}
