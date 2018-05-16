package test

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import util.GraphUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import util.OptionUtils._

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
  val personLikesPostEdgesPath = "src/main/resources/person_likes_post_0_0.csv"
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

    println("BI-QUERY1: ")
    val df1 = time(biQuery1("2008"))

    println("BI-QUERY2: ")
    val df2 = time(biQuery2("2008-02-13", "2018-02-12", "Belgium", "Denmark"))

    println("BI-QUERY3: ")
    val df3 = time(biQuery3(2010, 1))

    println("BI-QUERY4: ")
    val df4 = time(biQuery4("Philosopher", "Australia"))

    println("BI-QUERY5: ")
    val df5 = time(biQuery5("Greece"))

    println("BI-QUERY6: ")
    val df6 = time(biQuery6("Good_Vibrations"))

    println("BI-QUERY7: ")
    val df7 = time(biQuery7("Good_Vibrations"))

    println("BI-QUERY8: ")
    val df8 = time(biQuery8("Joe_Strummer"))

    println("BI-QUERY9: ")
    val df9 = time(biQuery8("Joe_Strummer"))

    println("BI-QUERY10: ")
    val df10 = time(biQuery10("Rock_You_Like_a_Hurricane", "2008-02-13"))

  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 1   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery1(messageYear: String): DataFrame = {

    // This import is needed to use the $-notation.
    import spark.implicits._

    // Union comment and post dataframes as they are both message types.
    val postTempDf = postVerticesDf.select($"id", $"creationDate", $"length")
    val commentTempDf = commentVerticesDf.select($"id", $"creationDate", $"length", $"replyOfPost")
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
      if (0 <= length && length < 40) 0
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
      .limit(100)

    personPlaceMessageTagDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 3   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery3(messageYear: Int, messageMonth: Int): DataFrame = {
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
      .limit(100)

    messageTagIdDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 4   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery4(tagName: String, countryName: String): DataFrame = {
    import spark.implicits._

    // From tag class side filter the classes by the name parameter and join them with the Tag and Post dataframes.
    val postDf = tagClassVerticesDf
      .select($"id" as "tag_class_id", $"name" as "tag_class_name")
      .filter($"tag_class_name".equalTo(tagName))
      .join(
        tagVerticesDf.withColumnRenamed("id", "tag_id"), $"hasType" === $"tag_class_id"
      )
      .select("tag_id")
      .join(
        postHasTagTagEdgesDf.withColumnRenamed("Post.id", "jt_post_id").withColumnRenamed("Tag.id", "jt_tag_id"),
        $"tag_id" === $"jt_tag_id"
      )
      .select($"jt_post_id")
      .join(
        postVerticesDf.withColumnRenamed("Forum.id", "post_forum_id"), $"id" === $"jt_post_id"
      )
      .select($"id" as "post_id", $"post_forum_id")

    // From the country side filter the country and join it with city, person and forum.
    val personDf = placeVerticesDf
      .filter($"name".equalTo(countryName))
      .select($"id" as "country_id")
      .join(
        placeVerticesDf, $"country_id" === $"isPartOf"
      )
      .select($"id" as "city_id")
      .join(
        personVerticesDf, $"place" === $"city_id"
      )
      .select($"id" as "person_id")

    // Now join the forum dataframe with persons and posts

    val forumDf = forumVerticesDf
      .join(
        postDf, $"id" === $"post_forum_id"
      )
      .distinct()
      .join(
        personDf, $"moderator" === $"person_id"
      )
      .groupBy($"id", $"title", $"creationDate", $"person_id")
      .agg(count("*") as "postCount")
      .orderBy(desc("postCount"), asc("id"))
      .select($"id" as "forum.id", $"title" as "forum.title", $"creationDate" as "forum.creationDate",
        $"person_id" as "person.id", $"postCount")
      .limit(20)

    forumDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 5   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery5(countryName: String): DataFrame = {
    import spark.implicits._

    // First find the most popular forum
    val forumDf = placeVerticesDf
      .filter($"name".equalTo(countryName))
      .select($"id" as "country_id")
      .join(
        placeVerticesDf, $"country_id" === $"isPartOf"
      )
      .select($"id" as "city_id")
      .join(
        personVerticesDf
          .withColumnRenamed("id", "person_id"),
        $"place" === $"city_id"
      )
      .join(
        forumHasMemberPersonEdgesDf
          .withColumnRenamed("Forum.id", "jt_forum_id")
          .withColumnRenamed("Person.id", "jt_person_id"),
        $"person_id" === $"jt_person_id"

      )
      .groupBy($"jt_forum_id")
      .agg(count("*") as "member")
      .orderBy(desc("member"), asc("jt_forum_id"))
      .limit(100)

    // Get the most popular forum members and count their posts in any forum.
    val memberDf = forumDf
      .join(
        forumHasMemberPersonEdgesDf
          .withColumnRenamed("Forum.id", "forumPerson_forum_id")
          .withColumnRenamed("Person.id", "forumPerson_person_id"), $"forumPerson_forum_id" === $"jt_forum_id"
      )
      .select($"forumPerson_person_id")
      .distinct()
      .join(
        personVerticesDf
          .withColumnRenamed("id", "person_id"), $"forumPerson_person_id" === $"person_id"
      )
      .select($"person_id", $"firstName", $"lastName", $"creationDate")

    val memberPostDf = memberDf
      .join(
        postVerticesDf
          .withColumnRenamed("id", "post_id")
          .withColumnRenamed("creationDate", "post_creationDate"),
          $"creator" === $"person_id"
      )
      .join(
        forumDf,
        $"jt_forum_id" === $"post_id"
      )
      .groupBy($"person_id", $"firstName", $"lastName", $"creationDate")
      .agg(count("*") as "postCount")
      .orderBy(desc("postCount"), asc("person_id"))
      .select($"person_id" as "person.id", $"firstName" as "person.firstName", $"lastName" as "person.lastName",
        $"creationDate" as "person.creationDate", $"postCount")
      .limit(100)
    memberPostDf
  }

  // ----------------------------------------------------------------------------------------------------------------
  // ----------------------------------------------   QUERY 6   -----------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------
  def biQuery6(tagName: String): DataFrame = {
    import spark.implicits._

    // Union comment and post dataframes as they are both message types.
    val postTempDf = postVerticesDf.select($"id", $"creator", $"length")
    val commentTempDf = commentVerticesDf.select($"id", $"creator", $"length")
    var messageDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)

    // Filter with tag name.
    val filteredDf = tagVerticesDf
      .withColumnRenamed("id", "tag_id")
      .filter($"name".equalTo(tagName))

    // Join tags with message join tables.
    val commentIdDf = filteredDf
      .join(
        commentHasTagTagEdgesDf
          .withColumnRenamed("Comment.id", "message_id")
          .withColumnRenamed("Tag.id", "c_tag_id"),
          $"tag_id" === $"c_tag_id"
      ).select($"message_id")

    val postIdDf = filteredDf
      .join(
        postHasTagTagEdgesDf
          .withColumnRenamed("Post.id", "message_id")
          .withColumnRenamed("Tag.id", "p_tag_id"),
        $"tag_id" === $"p_tag_id"
      ).select("message_id")

    // Get distinct messages.
    val selectedMessageDf = commentIdDf.union(postIdDf)
      .distinct()
      .join(
        messageDf,
        $"id" === $"message_id"
      )
      .select($"message_id", $"creator")


    // Get persons associated with these messages
    val personMessageDf = selectedMessageDf
      .join(
        personVerticesDf,
        $"creator" === $"id"
      )
      .select($"id" as "person_id", $"message_id")

    // Count likes.
    var personMessageDfWithLikes = personMessageDf
      .join(
        personLikesCommentEdgesDf
          .withColumnRenamed("Comment.id", "comment_id"),
        $"message_id" === $"comment_id",
        "left_outer"
      )
      .join(
        personLikesPostEdgesDf
          .withColumnRenamed("Post.id", "post_id"),
        $"message_id" === $"post_id",
        "left_outer"
      )

    val messageCountDf = personMessageDfWithLikes
        .withColumnRenamed("person_id", "agg_person_id")
      .groupBy("agg_person_id")
      .agg(count("*") as "messageCount")

    personMessageDfWithLikes = personMessageDfWithLikes
      .join(
        messageCountDf,
        $"person_id" === $"agg_person_id"
      )
      .groupBy($"person_id", $"message_id", $"messageCount")
      .agg(count("*") as "likeCount")

    val resultDf = personMessageDfWithLikes
      .join(
        commentVerticesDf,
        $"replyOfComment" === $"message_id" || $"replyOfPost" === $"message_id",
        "left_outer"
      )
      .groupBy($"person_id", $"message_id", $"messageCount", $"likeCount")
      .agg(count("*") as "replyCount")
      .withColumn("score", $"messageCount" * 1 + $"replyCount" * 2 + $"likeCount" * 10)
      .orderBy(desc("score"), asc("person_id"))
      .select($"person_id" as "person.id", $"replyCount", $"likeCount", $"messageCount", $"score")
      .limit(100)
    resultDf
  }

  def biQuery7(tagName: String): DataFrame = {
    import spark.implicits._

    // Union comment and post dataframes as they are both message types.
    val postTempDf = postVerticesDf.select($"id", $"creator", $"length")
    val commentTempDf = commentVerticesDf.select($"id", $"creator", $"length")
    var messageDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)

    // Filter with tag name.
    val filteredDf = tagVerticesDf
      .withColumnRenamed("id", "tag_id")
      .filter($"name".equalTo(tagName))

    // Join tags with message join tables.
    val commentIdDf = filteredDf
      .join(
        commentHasTagTagEdgesDf
          .withColumnRenamed("Comment.id", "message_id")
          .withColumnRenamed("Tag.id", "c_tag_id"),
        $"tag_id" === $"c_tag_id"
      ).select($"message_id")

    val postIdDf = filteredDf
      .join(
        postHasTagTagEdgesDf
          .withColumnRenamed("Post.id", "message_id")
          .withColumnRenamed("Tag.id", "p_tag_id"),
        $"tag_id" === $"p_tag_id"
      ).select("message_id")

    // Get distinct messages.
    val selectedMessageDf = commentIdDf.union(postIdDf)
      .distinct()
      .join(
        messageDf,
        $"id" === $"message_id"
      )
      .select($"message_id", $"creator")


    // Get persons associated with these messages
    val personMessageDf = selectedMessageDf
      .join(
        personVerticesDf,
        $"creator" === $"id"
      )
      .select($"id" as "person_id", $"message_id")

    // Find all person2. We already have the unioned messageDf, so start by joining
    //  messages with person_likes_ tables to get the liked ones.
    val person2Df = messageDf.withColumnRenamed("id", "message_id")
      .join(
        personLikesCommentEdgesDf.withColumnRenamed("Comment.id", "comment_id"),
        $"message_id" === $"comment_id"
      )
      .join(
        personLikesPostEdgesDf.withColumnRenamed("Post.id", "post_id"),
        $"message_id" === $"post_id",
        "left_outer"
      )
      .groupBy($"creator")
      .agg(count("*") as "popularityScore")
      .withColumnRenamed("creator", "person2_id")

    // Union person_like tables
    val personLikesMessageDf = GraphUtils.unionDifferentTables(
      personLikesCommentEdgesDf
        .withColumnRenamed("Person.id", "person_id")
        .withColumnRenamed("Comment.id", "message_id")
        .drop($"creationDate"),
      personLikesPostEdgesDf
        .withColumnRenamed("Person.id", "person_id")
        .withColumnRenamed("Post.id", "message_id")
        .drop($"creationDate")
    )

    // Now find get the messages liked by person2s. Then join them with persons and tags.
    val person2LikedMessagesDf = person2Df.withColumnRenamed("person2_id", "person2_id")
      .join(
        personLikesMessageDf,
        $"person2_id" === $"person_id"
      )
      .groupBy($"message_id")
      .agg(sum($"popularityScore") as "authorityScore")

    // Join the messages with authority score with messages table to get creator.
    // Then join with persons


    person2LikedMessagesDf
      .join(
        messageDf,
        $"creator" === $"message_id"
      )
      .join(
        personMessageDf.withColumnRenamed("message_id", "p_message_id"),
        $"message_id" === $"p_message_id"
      )
      .groupBy($"person_id")
      .agg(sum($"authorityScore") as "authorityScore")
      .orderBy(desc("authorityScore"), asc("person_id"))
      .select($"person_id" as "person.id", $"authorityScore")
      .limit(100)
  }

  def biQuery8(tagName: String): DataFrame = {
    import spark.implicits._

    // Union comment and post dataframes as they are both message types.
    val postTempDf = postVerticesDf.select($"id", $"creator", $"length")
    val commentTempDf = commentVerticesDf.select($"id", $"creator", $"length")
    var messageDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)

    // Filter with tag name.
    val filteredDf = tagVerticesDf
      .withColumnRenamed("id", "tag_id")
      .filter($"name".equalTo(tagName))

    // Join tags with message join tables.
    val commentIdDf = filteredDf
      .join(
        commentHasTagTagEdgesDf
          .withColumnRenamed("Comment.id", "message_id")
          .withColumnRenamed("Tag.id", "c_tag_id"),
        $"tag_id" === $"c_tag_id"
      ).select($"message_id")

    val postIdDf = filteredDf
      .join(
        postHasTagTagEdgesDf
          .withColumnRenamed("Post.id", "message_id")
          .withColumnRenamed("Tag.id", "p_tag_id"),
        $"tag_id" === $"p_tag_id"
      ).select("message_id")

    // Get distinct messages.
    val selectedMessageDf = commentIdDf.union(postIdDf)
      .distinct()
      .join(
        messageDf,
        $"id" === $"message_id"
      )
      .select($"message_id", $"creator")

    val commentDf = selectedMessageDf
      .join(
        commentVerticesDf,
        $"replyOfPost" === $"message_id" || $"replyOfComment" === $"message_id"
      )

    // Join commentHasTagTag with Tags and filter it ccording to the query description.
    val filteredTags = commentHasTagTagEdgesDf
      .withColumnRenamed("Comment.id", "comment_id")
      .withColumnRenamed("Tag.id", "tag_id")
      .join(
        tagVerticesDf,
        $"tag_id" === $"id"
      )
      .filter($"name".notEqual(tagName))
      .select($"tag_id" as "filtered_tag_id", $"name")

    // Join commentDf with commentHasTagTagDf then with filteredTags.
    val filteredCommentDf = commentDf
      .join(
        commentHasTagTagEdgesDf
          .withColumnRenamed("Comment.id", "comment_id")
          .withColumnRenamed("Tag.id", "tag_id"),
        $"id" === $"comment_id"

      )
      .join(
        filteredTags,
        $"tag_id" === $"filtered_tag_id"
      )
      .groupBy($"name")
      .agg(count("*") as "count")
      .orderBy(desc("count"), asc("name"))
      .select($"name" as "relatedTag.name", $"count")
      .limit(100)

    filteredCommentDf
  }

  def biQuery9(tagClass1: String, tagClass2: String, forumLimit: Int) : DataFrame = {
    import spark.implicits._

    val tagClass1Df = tagClassVerticesDf.filter($"name".equalTo(tagClass1))
    val tagClass2Df = tagClassVerticesDf.filter($"name".equalTo(tagClass2))

    val tag1Df = tagVerticesDf
      .withColumnRenamed("id", "tag1_id")
      .join(
        tagClass1Df
          .withColumnRenamed("id", "tagclass_id"),
        $"hasType" === $"tagclass_id"
      )
      .select("tag1_id")

    val tag2Df = tagVerticesDf
      .withColumnRenamed("id", "tag2_id")
      .join(
        tagClass2Df
          .withColumnRenamed("id", "tagclass_id"),
        $"hasType" === $"tagclass_id"
      )
      .select("tag2_id")

    val forumWithTagsDf = forumVerticesDf
      .withColumnRenamed("id", "forum_id")
      .join(
        postVerticesDf
          .withColumnRenamed("id", "post_id")
          .withColumnRenamed("Forum.id", "post_forum_id"),
        $"forum_id" === $"post_forum_id"
      )
      .join(
        postHasTagTagEdgesDf
          .withColumnRenamed("Post.id", "posttag_post_id")
          .withColumnRenamed("Tag.id", "posttag_tag_id"),
        $"posttag_post_id" === $"post_id"
      )

    val forum1Df = forumWithTagsDf
      .join(
        tag1Df,
        $"tag1_id" === $"posttag_tag_id"
      )
      .groupBy($"forum_id")
      .agg(count("*") as "count1")
      .select("forum_id", "count1")
      .filter($"count1" >= forumLimit)

    val forum2Df = forumWithTagsDf
      .join(
        tag2Df,
        $"tag2_id" === $"posttag_tag_id"
      )
      .groupBy($"forum_id")
      .agg(count("*") as "count2")
      .select("forum_id", "count2")
      .filter($"count2" >= forumLimit)

    val result = GraphUtils.unionDifferentTables(forum1Df, forum2Df)
      .withColumn("count1", when($"count1".isNull, 0).otherwise($"count1"))
      .withColumn("count2", when($"count2".isNull, 0).otherwise($"count2"))
      .orderBy(abs($"count2" - $"count1").desc)
      .orderBy(asc("forum_id"))
      .withColumnRenamed("forum_id", "forum.id")
      .limit(100)

    result
  }

  def biQuery10(tagName: String, messageDate: String) : DataFrame = {
    import spark.implicits._

    val filteredTagDf = tagVerticesDf.filter($"name".equalTo(tagName))

    val interestedPersonDf = personHasInterestTagEdgesDf
      .withColumnRenamed("Person.id", "person_id")
      .withColumnRenamed("Tag.id", "tag_id")
      .join(
        filteredTagDf,
        $"id" === $"tag_id"
      )
      .select("person_id")
      .withColumn("score1", lit(100))

    val postTempDf = postVerticesDf.select($"id", $"creationDate", $"creator", $"length")
    val commentTempDf = commentVerticesDf.select($"id", $"creationDate", $"creator", $"length")
    val messageDf = GraphUtils.unionDifferentTables(postTempDf, commentTempDf)
      .withColumn("creationDate", expr("substring(creationDate, 1, length(creationDate)-18)"))
      .withColumn("creationDate", to_date($"creationDate", "yyyy-mm-dd"))
      .filter($"creationDate" > messageDate)

    val messageTagJtDf = GraphUtils
      .unionDifferentTables(
        commentHasTagTagEdgesDf
          .withColumnRenamed("Comment.id", "mt_message_id")
          .withColumnRenamed("Tag.id", "mt_tag_id"),
        postHasTagTagEdgesDf
          .withColumnRenamed("Post.id", "mt_message_id")
          .withColumnRenamed("Tag.id", "mt_tag_id")
      )

    val filteredTagMessageDf = messageDf
      .withColumnRenamed("id", "message_id")
      .join(
        messageTagJtDf,
        $"message_id" === $"mt_message_id"
      )

    val allPersonDf = filteredTagMessageDf
      .groupBy($"creator")
      .agg(count("*") as "score2")

    val personResultDf = GraphUtils.unionDifferentTables(
      allPersonDf
        .withColumnRenamed("creator", "person_id"),
      interestedPersonDf
    )
      .withColumn("score1", when($"score1".isNull, 0).otherwise($"score1"))
      .withColumn("score2", when($"score2".isNull, 0).otherwise($"score2"))
      .withColumn("score", $"score1" + $"score2")

      val result = personResultDf
      .join(
        personKnowsPersonEdgesDf
          .withColumnRenamed("Person1.id", "person1_id")
          .withColumnRenamed("Person2.id", "person2_id"),
          $"person_id" === $"person1_id",
          "left_outer"
      )
      .join(
        personResultDf
          .withColumnRenamed("person_id", "jt_person_id")
          .withColumnRenamed("score", "friendsScore"),
        $"person2_id" === $"jt_person_id"
      )
      .groupBy("person_id", "score")
      .agg(sum("friendsScore") as "friendsScore")
      .orderBy(($"score" + $"friendsScore").desc)
      .orderBy(asc("person_id"))
      .select($"person_id" as "person.id", $"score", $"friendsScore")
      .limit(100)

    result
  }
}
