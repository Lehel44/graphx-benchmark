package test

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions.{lit, udf}

object GraphFrameTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

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
    val commentVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentVerticesPath)
    val forumVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumVerticesPath)
    val organisationVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(organisationVerticesPath)
    val personVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personVerticesPath)
    val placeVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(placeVerticesPath)
    val postVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(postVerticesPath)
    val tagVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(tagVerticesPath)
    val tagClassVerticesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(tagClassVerticesPath)

    /**
      * Edge rdds.
      */
    val commentHasTagTagEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(commentHasTagTagEdgesPath)
    val forumHasMemberPersonEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumHasMemberPersonEdgesPath)
    val forumHasTagTagEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(forumHasTagTagEdgesPath)
    val personHasInterestTagEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personHasInterestTagEdgesPath)
    val personKnowsPersonEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personKnowsPersonEdgesPath)
    val personLikesCommentEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personLikesCommentEdgesPath)
    val personLikesPostEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personLikesPostEdgesPath)
    val personStudyAtOrganisationEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personStudyAtOrganisationEdgesPath)
    val personWorkAtOrganisationEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(personWorkAtOrganisationEdgesPath)
    val postHasTagTagEdgesDf = spark.read.format("csv").options(SCHEMA_OPTIONS).load(postHasTagTagEdgesPath)


    import spark.implicits._

    /*
    val commentJoinedToCommentHasTag = commentVerticesDf.select($"id" as "Comment_id", struct($"creationDate", $"locationIP", $"browserUsed", $"content", $"length",
      $"creator", $"place", $"replyOfPost", $"replyOfComment") as "Comment")
      .join(commentHasTagTagEdgesDf.withColumnRenamed("Comment.id", "Com_id"), $"Comment_id" === $"Com_id", "fullouter")
      .drop($"Com_id")
      .withColumnRenamed("Tag.id", "Tag_junction_id")


    val commentJoinedToTag = commentJoinedToCommentHasTag
      .join(tagVerticesDf.select($"id" as "Tag_id", struct($"name", $"url", $"hasType") as "Tag"), $"Tag_junction_id" === $"Tag_id", "fullouter")
      .drop("Tag_junction_id").withColumn("id", monotonically_increasing_id())
     */

    //commentJoinedToTag.show()
    // A probléma: Full outer joinokkal elkészítjük a csúcsokat. A csúcsok között többszörös élek futnak különböző éltípusokkal.
    // Hogyan vesszük fel az éleket a csúcstábla alapján? A csúcstáblának generálni kell egy id oszlopot a végén. Pl:
    // forum_hasMember_person : 21, 15, 2018-10-01. Forumid inner join, Person id inner join, Select Distinct Id -> Kész
    // DE! Igazából mindig ott lesz él, ahol a teljes sor ki van töltve, tehát hurokélek lesznek. -> A kapcsolatokat
    // ne vigyük bele a csúcstáblába.
    /*
    commentJoinedToTag.join(commentHasTagTagEdgesDf.withColumnRenamed("Comment.id", "Com_id_right").withColumnRenamed("Tag.id", "Tag_id_right"),
      $"Comment_id" === $"Com_id_right", "inner").select("id").distinct().show()

    commentJoinedToTag.join(commentHasTagTagEdgesDf.withColumnRenamed("Comment.id", "Com_id_right").withColumnRenamed("Tag.id", "Tag_id_right"),
      $"Tag_id" === $"Tag_id_right", "inner").select("id").distinct().show()
     */


    val structuredComment = commentVerticesDf.select(struct($"id" as "comment_id", $"creationDate", $"locationIP", $"browserUsed", $"content", $"length",
      $"creator", $"place", $"replyOfPost", $"replyOfComment") as "Comment")

    val structuredTag = tagVerticesDf.select(struct($"id" as "tag_id", $"name", $"url", $"hasType") as "Tag")

    val commentJoinedToTag = structuredComment
      .withColumn("null_col", lit(null: String))
      .join(structuredTag, $"null_col" === "tag_id", "fullouter")
      .drop("null_col")
      .withColumn("id", monotonically_increasing_id())

    //commentJoinedToTag.select($"id", $"Comment.*", $"Tag.*")
    //.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("commentJoinedToTag.csv")

    val commentTagEdgeLeftSide = commentJoinedToTag
      .join(commentHasTagTagEdgesDf
      .withColumnRenamed("Comment.id", "comment_junction_id"),
        $"comment_junction_id" === $"Comment.comment_id")
      .select("id", "Comment.comment_id")
    commentTagEdgeLeftSide.show()

    //commentJoinedToTag.join(commentTagEdgeLeftSide.withColumnRenamed("id", "join_id"),
    //$"join_id" === $"")


    /*val joined = commentVerticesDf.select(struct($"id" as "comment_id", $"creationDate", $"locationIP", $"browserUsed", $"content", $"length",
      $"creator", $"place", $"replyOfPost", $"replyOfComment") as "Comment")
      .withColumn("null_name", lit(null: String))
      .join(tagVerticesDf.
        withColumnRenamed("id", "tag_id"), $"null_name" === $"name", "fullouter")
      .drop("null_name")
    joined.show()*/

    //joined.select($"id", $"Comment.*", $"Tag.*")
    //.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("commentJoinedToTag.csv")
  }


}

