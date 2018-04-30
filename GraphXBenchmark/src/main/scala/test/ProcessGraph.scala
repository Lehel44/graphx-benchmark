package test

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import util.GraphUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._


//sealed trait EdgeProperty

object ProcessGraph {

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
    * Vertex dataframes.
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
    * Edge dataframes.
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

  def main(args: Array[String]): Unit = {



   /* val startDate = "2008-02-13"
    val endDate = "2018-02-12"
    val country1 = "Nugegoda"
    val country2 = "Chief"

    val df = biQuery2(startDate, endDate, country1, country2)
    df.show()*/

    val df3 = biQuery3(2010, 1)
    df3.show()
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 1   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery1(spark: SparkSession, commentDf: DataFrame, postDf: DataFrame, messageYear: String): DataFrame = {

    // This import is needed to use the $-notation.
    import spark.implicits._

    // Union comment and post dataframes as they are both message types.
    val postTempDf = postDf.select($"id", $"creationDate", $"length")
    val commentTempDf = commentDf.select($"id", $"creationDate", $"length", $"replyOfPost")
    var commentPostDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)

    // Transforms timestamps to date to let spark be able to handle it. Then extracts the
    // year of the date and filters it.
    commentPostDf = commentPostDf
      .withColumn("creationDate", expr("substring(creationDate, 1, length(creationDate)-18)"))
      .withColumn("creationDate", to_date($"creationDate", "yyyy-mm-dd"))
      .filter($"creationDate" < messageYear)
      .withColumn("year", year($"creationDate"))



    // UDF for year group types based on length
    val lengthCategory = udf((length: Int) => {
      if (0 <= length && length < 40 ) 0
      else if (0 <= length && length < 80) 1
      else if (80 <= length && length < 160) 2
      else if (160 <= length) 3
      else -1
    })

    // add yearType group column for grouping
    commentPostDf = commentPostDf.withColumn("lengthCategory", lengthCategory($"length"))

    // process grouping
    commentPostDf = commentPostDf
      .groupBy($"year", $"replyOfPost".isNotNull as "isComment", $"lengthCategory")
      .agg(count("*") as "messageCount", avg("length") as "averageMessageLength", sum("length") as "sumMessageLength",
        ((count("*") * 100) / commentPostDf.count()) as "percentageOfMessages")

    commentPostDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 2   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------

  def biQuery2(startDate: String, endDate: String, country1: String, country2: String): DataFrame = {
    import spark.implicits._
    // Union comment and post dataframes as they are both message types.
    val postTempDf = postVerticesDf.select($"id", $"creationDate", $"length", $"creator")
    val commentTempDf = commentVerticesDf.select($"id", $"creationDate", $"length", $"creator", $"replyOfPost")
    var messageDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)

    // Transforms timestamps to date to let spark be able to handle it. Then extracts the
    // year of the date.
    messageDf = messageDf
      .withColumn("creationDate", expr("substring(creationDate, 1, length(creationDate)-18)"))
      .withColumn("creationDate", to_date($"creationDate", "yyyy-mm-dd"))
      .filter($"creationDate" >= startDate && "$creationDate" <= endDate)

    // The place dataframe stores cities and countries. A city has a partOf column that refers to its country.
    // Join the person and place dataframes and create a column that contains only countries belong to each persons.

    var personPlaceDf = personVerticesDf.as("person").withColumnRenamed("id", "person_id")
      .join(placeVerticesDf
        .filter($"name".equalTo(country1) || $"name".equalTo(country2)).as("place")
        .withColumnRenamed("id", "place_id")
        .withColumnRenamed("name", "place_name"), $"person.place" === $"place_id")

    // UDF to get country from city
    val getCountry = udf((placeId: Int, placeType: String, isPartOf: Int) => {
      if (placeType == "city") isPartOf
      else if (placeType == "country") placeId
      else -1
    })

    personPlaceDf = personPlaceDf
      .withColumn("country", getCountry($"place_id", $"type", $"isPartOf"))
      .select("person_id", "gender", "birthday", "place_name")

    // Join message dataframe with persons on the creatorOf column.
    val personPlaceMessageDf = personPlaceDf
      .join(messageDf.as("message")
        .withColumnRenamed("id", "message_id"), $"person_id" === $"message.creator")

    // Join the current dataframe with tags on the hasTag column, then select the requires ones.
    // the join tables are different for comments and posts. After the inner joins the union give the final
    // redundant dataframe.
    val personPlaceCommentTagIdDf = personPlaceMessageDf.as("ppm")
      .join(commentHasTagTagEdgesDf.as("comment_tag")
        .withColumnRenamed("Comment.id", "comment_id")
        .withColumnRenamed("Tag.id", "tag_id"), $"ppm.message_id" === $"comment_id")

    val personPlacePostTagIdDf = personPlaceMessageDf.as("ppm")
      .join(postHasTagTagEdgesDf.as("post_tag")
        .withColumnRenamed("Post.id", "post_id")
        .withColumnRenamed("Tag.id", "tag_id"), $"ppm.message_id" === $"post_id")

    val personPlaceMessageIdTagDf = personPlaceCommentTagIdDf.union(personPlacePostTagIdDf)

    // Joining with tag table to get tag names and creating the month column.
    var personPlaceMessageTagDf = personPlaceMessageIdTagDf
      .join(tagVerticesDf.as("tags")
        .withColumnRenamed("id", "tag_class_id")
        .withColumnRenamed("name", "tag_name"), $"tag_id" === $"tag_class_id")
      .select($"person_id" as "id", $"place_name", $"gender", $"birthday", $"creationDate", $"tag_name")
      .withColumn("messageMonth", month($"creationDate"))

    // UDF for age groups
    val getAgeGroup = udf((birthday: String) => {
      import java.time._
      val dateString = birthday.slice(0, 10)
      val birthDate = LocalDate.parse(dateString)
      val simulationEndDate = LocalDate.parse("2013-01-01")

      val p = Period.between(birthDate, simulationEndDate)
      p.getYears / 5
    })

    personPlaceMessageTagDf = personPlaceMessageTagDf.withColumn("ageGroup", getAgeGroup($"birthday"))

    // Group by 5-level criteria
    personPlaceMessageTagDf = personPlaceMessageTagDf
      .groupBy($"place_name", $"messageMonth", $"gender", $"ageGroup", $"tag_name")
      .agg(count("*") as "messageCount")
      .filter($"messageCount" > 1)
      .orderBy(desc("messageCount"), asc("tag_name"), asc("ageGroup"), asc("gender"), asc("messageMonth"), asc("place_name"))
      .select($"place_name" as "country.name", $"messageMonth", $"gender" as "person.gender", $"ageGroup",
        $"tag_name" as "tag.name", $"messageCount")

    personPlaceMessageTagDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 2   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery3(messageYear: Int, messageMonth: Int) : DataFrame = {
    import spark.implicits._

    // Count next month and year
    val nextMessageYear = messageYear + messageMonth / 12
    val nextMessageMonth = (messageMonth + 1) % 12

    // From the commentHasTagTag and postHasTag dataframes selects the tag id, creates a union of them.
    val commentTagIds = commentHasTagTagEdgesDf
      .withColumnRenamed("Comment.id", "message_id").withColumnRenamed("Tag.id", "tag_id")
    val postTagIds = postHasTagTagEdgesDf
      .withColumnRenamed("Comment.id", "message_id").withColumnRenamed("Tag.id", "tag_id")
    val messageTagIds = commentTagIds.union(postTagIds)

    // Filter the messages with message year and month
    val commentTempDf = commentVerticesDf.select($"id", $"creationDate")
    val postTempDf = postVerticesDf.select($"id", $"creationDate")
    val messageDf = commentTempDf.union(postTempDf)
      .withColumn("creationDate", expr("substring(creationDate, 1, length(creationDate)-18)"))
      .withColumn("creationDate", to_date($"creationDate", "yyyy-mm-dd"))
      .filter(year($"creationDate").equalTo(messageYear) || year($"creationDate").equalTo(nextMessageYear))
      .filter(month($"creationDate").equalTo(messageMonth) || month($"creationDate").equalTo(nextMessageMonth))

    // Inner join the messages with messageTagIds and the result with tags. Then group it
    //(month($"creationDate").equalTo(messageMonth)) as "month1", (month($"creationDate").equalTo(nextMessageMonth)) as "month2"
    val messageTagIdDf = messageDf
      .join(messageTagIds, $"id" === $"message_id")
      .join(tagVerticesDf.withColumnRenamed("id", "tag_table_id"), $"tag_id" === $"tag_table_id")
      .groupBy($"name", month($"creationDate") as "month")
      .agg(
        count(when(month($"creationDate").equalTo(messageMonth), 1)) as "countMonth1",
        count(when(month($"creationDate").equalTo(nextMessageMonth), 1)) as "countMonth2"
      )
      .withColumn("diff", abs($"countMonth1" - $"countMonth2"))
      .orderBy(desc("diff"), asc("name"))
      .withColumnRenamed("name", "tag.name")
      .drop($"month")

      messageTagIdDf
  }
}
