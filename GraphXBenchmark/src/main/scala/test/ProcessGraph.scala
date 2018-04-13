package test

import model.{CommentProperty, ForumProperty, PersonProperty, VertexProperty}
import model.edge.EdgeProperty
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.graphframes.GraphFrame
import process.{EdgeProcessor, VertexProcessor}
import util.OptionUtils

//sealed trait EdgeProperty

object ProcessGraph {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkQueries")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val SCHEMA_OPTIONS = Map("header" -> "true", "inferSchema" -> "true", "sep" -> "|")

    /**
      * Vertex paths.
      */
    val commentVerticesPath = "src/main/resources/comment_0_0.csv"
    val forumVerticesPath = "src/main/resources/forum_0_0.csv"
    val organisationVerticesPath = "src/main/resources/organisation_0_0.csv"
    val personVerticesPath = "src/main/resources/person_0_0.csv"
    val placeVerticesPath = "src/main/resources/place_0_0.csv"
    val postVerticesPath = "src/main/resources/post_0_0.csv"
    val tagVerticesPath = "src/main/resources/tag_0_0.csv"
    val tagClassVerticesPath = "src/main/resources/tagclass_0_0.csv"

    /**
      * Edge paths.
      */
    val commentHasTagTagEdgesPath = "src/main/resources/comment_hasTag_tag_0_0.csv"
    val forumHasMemberPersonEdgesPath = "src/main/resources/forum_hasMember_person_0_0.csv"
    val forumHasTagTagEdgesPath = "src/main/resources/forum_hasTag_tag_0_0.csv"
    val personHasInterestTagEdgesPath = "src/main/resources/person_hasInterest_tag_0_0.csv"
    val personKnowsPersonEdgesPath = "src/main/resources/person_knows_person_0_0.csv"
    val personLikesCommentEdgesPath = "src/main/resources/person_likes_comment_0_0.csv"
    val personLikesPostEdgesPath = "src/main/resources/person_likes_comment_0_0.csv"
    val personStudyAtOrganisationEdgesPath = "src/main/resources/person_studyAt_organisation_0_0.csv"
    val personWorkAtOrganisationEdgesPath = "src/main/resources/person_workAt_organisation_0_0.csv"
    val postHasTagTagEdgesPath = "src/main/resources/post_hasTag_tag_0_0.csv"

    /**
      * Vertex rdds.
      */
    val commentVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentVerticesPath).rdd
    val forumVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumVerticesPath).rdd
    val organisationVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(organisationVerticesPath).rdd
    val personVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personVerticesPath).rdd
    val placeVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(placeVerticesPath).rdd
    val postVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(postVerticesPath).rdd
    val tagVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(tagVerticesPath).rdd
    val tagClassVerticesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(tagClassVerticesPath).rdd

    /**
      * Edge rdds.
      */
    val commentHasTagTagEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentHasTagTagEdgesPath).rdd
    val forumHasMemberPersonEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumHasMemberPersonEdgesPath).rdd
    val forumHasTagTagEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumHasTagTagEdgesPath).rdd
    val personHasInterestTagEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personHasInterestTagEdgesPath).rdd
    val personKnowsPersonEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personKnowsPersonEdgesPath).rdd
    val personLikesCommentEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personLikesCommentEdgesPath).rdd
    val personLikesPostEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personLikesPostEdgesPath).rdd
    val personStudyAtOrganisationEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personStudyAtOrganisationEdgesPath).rdd
    val personWorkAtOrganisationEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personWorkAtOrganisationEdgesPath).rdd
    val postHasTagTagEdgesRdd = spark.read.format("csv").options(SCHEMA_OPTIONS).load(postHasTagTagEdgesPath).rdd

    /**
      * Processed vertex rdds.
      */
    val commentRdd = VertexProcessor.createCommentVertexRdd(commentVerticesRdd)
    val forumRdd = VertexProcessor.createForumVertexRdd(forumVerticesRdd)
    val organisationRdd = VertexProcessor.createOrganisationVertexRdd(organisationVerticesRdd)
    val personRdd = VertexProcessor.createPersonVertexRdd(personVerticesRdd)
    val placeRdd = VertexProcessor.createPlaceVertexRdd(placeVerticesRdd)
    val postRdd = VertexProcessor.createPostVertexRdd(postVerticesRdd)
    val tagRdd = VertexProcessor.createTagVertexRdd(tagVerticesRdd)
    val tagClassRdd = VertexProcessor.createTagClassVertexRdd(tagClassVerticesRdd)

    /**
      * Processed edge rdds.
      */
    val commentHasTagTagRdd = EdgeProcessor.createCommentHasTagEdgeRdd(commentHasTagTagEdgesRdd)
    val forumHasMemberPersonRdd = EdgeProcessor.createForumHasMemberPersonEdgeRdd(forumHasMemberPersonEdgesRdd)
    val forumHasTagTagRdd = EdgeProcessor.createForumHasTagTagEdgeRdd(forumHasTagTagEdgesRdd)
    val personHasInterestTagRdd = EdgeProcessor.createPersonHasInterestTagEdgeRdd(personHasInterestTagEdgesRdd)
    val personKnowsPersonRdd = EdgeProcessor.createPersonKnowsPersonEdgeRdd(personKnowsPersonEdgesRdd)
    val personLikesCommentRdd = EdgeProcessor.createPersonLikesCommentEdgeRdd(personLikesCommentEdgesRdd)
    val personLikesPostRdd = EdgeProcessor.createPersonLikesPostEdgeRdd(personLikesPostEdgesRdd)
    val personStudyAtOrganisationRdd = EdgeProcessor.createPersonStudyAtOrganisationEdgeRdd(personStudyAtOrganisationEdgesRdd)
    val personWorkAtOrganisationRdd = EdgeProcessor.createPersonWorkAtOrganisationEdgeRdd(personWorkAtOrganisationEdgesRdd)
    val postHasTagTagRdd = EdgeProcessor.createPostHasTagTagEdgeRdd(postHasTagTagEdgesRdd)

    /**
      * Unified vertices.
      */
    val unifiedVertices: RDD[(VertexId, VertexProperty)] =
      commentRdd
        .union(forumRdd)
        .union(organisationRdd)
        .union(personRdd)
        .union(placeRdd)
        .union(postRdd)
        .union(tagRdd)
        .union(tagClassRdd)

    /**
      * Unified edges.
      */
    val unifiedEdges: RDD[Edge[EdgeProperty]] =
      commentHasTagTagRdd
          .union(forumHasMemberPersonRdd)
      .union(forumHasTagTagRdd)
      .union(personHasInterestTagRdd)
      .union(personKnowsPersonRdd)
      .union(personLikesCommentRdd)
      .union(personLikesPostRdd)
      .union(personStudyAtOrganisationRdd)
      .union(personWorkAtOrganisationRdd)
      .union(postHasTagTagRdd)

    // FORUM TRANSFORM
    val forumVerticesDF = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumVerticesPath)
    val forumVerticesThing = forumVerticesDF.collect().map(_.toSeq.map(_.toString))

    val forumObjects = forumVerticesThing.map(s => (s(0).toString.toLong, ForumProperty(OptionUtils.toSomeString(s(1)), OptionUtils.toSomeString(s(2)),
      OptionUtils.toSomeLong(OptionUtils.toSomeString(s(3)))))).toSeq


    val forumSeq: Seq[(Long, VertexProperty)] = forumObjects
    val forumDF = spark.createDataset(forumSeq)(Encoders.tuple(Encoders.scalaLong, Encoders.kryo[VertexProperty])).toDF("id", "object")

    //forumDS.foreach(p => println(p._2.asInstanceOf[ForumProperty].title))

  // PERSON TRANSFORM

    val personVerticesDF = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personVerticesPath)
    val personVerticesThing = personVerticesDF.collect().map(_.toSeq.map(_.toString))

    val personObjects = personVerticesThing.map(s => (s(0).toString.toLong, PersonProperty(
      OptionUtils.toSomeString(s(1)),
      OptionUtils.toSomeString(s(2)),
      OptionUtils.toSomeString(s(3)),
      OptionUtils.toSomeString(s(4)),
      OptionUtils.toSomeString(s(5)),
      OptionUtils.toSomeString(s(6)),
      OptionUtils.toSomeString(s(7)),
      OptionUtils.toSomeLong(OptionUtils.toSomeString(s(8))),
      OptionUtils.toSomeString(s(9)),
      OptionUtils.toSomeString(s(10))
    ))
    ).toSeq

    val personSeq: Seq[(Long, VertexProperty)] = personObjects
    val personDF = spark.createDataset(personSeq)(Encoders.tuple(Encoders.scalaLong, Encoders.kryo[VertexProperty])).toDF("id", "object")




    val unionShitDF = forumDF.union(personDF).toDF
    //unionShitDF.printSchema()

    // FORUM - PERSON EDGES

    val forumHasMemberPersonEdgesDF = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumHasMemberPersonEdgesPath).toDF("src", "dst", "type")

    val gf = GraphFrame(unionShitDF, forumHasMemberPersonEdgesDF)

    gf.vertices.show()


    //import spark.implicits._
    //implicit val VertexKryoEncoder = Encoders.kryo[VertexProperty]
    //implicit val CommentKryoEncoder = Encoders.kryo[CommentProperty]
    //val df = commentRdd.toDF()

    //val schema = OptionUtils.dfCommentSchema(List("title", "creationDate", "moderator"))

    //val graph = Graph(personRdd, personKnowsPersonRdd)
//    graph.edges.take(10).foreach(println)

    //val g: GraphFrame = GraphFrame.fromGraphX(graph)

    //g.vertices.show(10)

  }


}
