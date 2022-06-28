import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

object Stats {

  // Kaggle link: https://www.kaggle.com/datasets/justinas/nba-players-data

  val playerSchema = StructType(Array(
    StructField("index", IntegerType, nullable = true),
    StructField("player_name", StringType, nullable = true),
    StructField("team_abbreviation", StringType, nullable = true),
    StructField("age", FloatType, nullable = true),
    StructField("player_height", DoubleType, nullable = true),
    StructField("player_weight", DoubleType, nullable = true),
    StructField("college", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("draft_year", StringType, nullable = true),
    StructField("draft_round", StringType, nullable = true),
    StructField("draft_number", StringType, nullable = true),
    StructField("gp", IntegerType, nullable = true),
    StructField("pts", DoubleType, nullable = true),
    StructField("reb", DoubleType, nullable = true),
    StructField("ast", DoubleType, nullable = true),
    StructField("net_rating", DoubleType, nullable = true),
    StructField("oreb_pct", DoubleType, nullable = true),
    StructField("dreb_pct", DoubleType, nullable = true),
    StructField("usg_pct", DoubleType, nullable = true),
    StructField("ts_pct", DoubleType, nullable = true),
    StructField("ast_pct", DoubleType, nullable = true),
    StructField("season", StringType, nullable = true),
  ))

  // Find Count of Null on All DataFrame Columns
  def findNullColumnsCount(columns: Array[String]): Array[Column] = {
    columns.map((c) => {
      count(when(col(c).isNull, c)).alias(c)
    })
  }

  // Get all unique values for Teams
  def getUniqueTeamNames(playersDF: DataFrame): Array[String] = {
    playersDF.select("team_abbreviation").distinct().rdd.map(row => row(0).toString).collect()
  }

  def removeNulls(playersDF: DataFrame) = {
    playersDF.filter(row => !row.anyNull) // Return rows with no null values
  }

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("NBA_Stats_1996-2021")
      .master("local[*]") // Use all local machine's CPU cores
      .getOrCreate()

    // Read csv file that contains stats for NBA players from 1996-2020
    val playersStats = spark.read // DataFrame object
      .option("header", "true")
      .schema(playerSchema)
      .format("csv")
      .load("data/all_seasons.csv")

    // Print number of null values for each column
    playersStats.select(findNullColumnsCount(playersStats.columns):_*).show()

    val cleanedPlayersStats = removeNulls(playersStats)
    val teams = getUniqueTeamNames(cleanedPlayersStats)

    println("Enter your team's short name (example - GSW, CLE, LAL): ")
    val myTeam = scala.io.StdIn.readLine(); // Read user's input - their team
    if(!teams.contains(myTeam.trim)) { // Check if input is a valid team name
      println("Invalid team name!")
      return
    }

    // Get total points, rebounds and assists for players from my team
    val myPlayersStatsTotal = cleanedPlayersStats
      .filter(player => player.getAs("team_abbreviation") == myTeam) // Select only players from user's team
      .withColumn("total_points_season", col("pts") * col("gp")) // Total points in a season = points per game in a season * number of games played in a season
      .withColumn("total_rebounds_season", col("reb") * col("gp"))
      .withColumn("total_assists_season", col("ast") * col("gp"))
      .select("player_name","total_points_season", "total_rebounds_season", "total_assists_season", "gp")
      .groupBy("player_name")
      .agg( // totals for all players
        sum("total_points_season").alias("total_points"),
        sum("total_rebounds_season").alias("total_rebounds"),
        sum("total_assists_season").alias("total_assists"),
        sum("gp").alias("total_games"),
      )

    // Calculate points, rebounds and assists averages over career in user's team
    val myPlayersStatsPerGame = myPlayersStatsTotal
      .withColumn("ppg", round((col("total_points") / col("total_games")),1)) // Points per game = total points / total games played
      .withColumn("rpg", round((col("total_rebounds") / col("total_games")),1))
      .withColumn("apg", round((col("total_assists") / col("total_games")),1))
      .select("player_name", "ppg", "rpg", "apg", "total_games")
      .orderBy(desc("ppg"))

    myPlayersStatsPerGame.show()

    // Save DataFrame to new csv file
    myPlayersStatsPerGame.write.option("header", true).csv("data/my_best_players")
  }
}
