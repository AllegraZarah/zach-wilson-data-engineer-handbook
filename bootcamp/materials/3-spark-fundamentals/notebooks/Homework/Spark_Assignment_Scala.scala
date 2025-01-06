import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{broadcast, split, lit}

// Initialize a Spark Session
val sparkSession = SparkSession.builder.appName("Assignment3").getOrCreate()

// Task 1: Disable automatic broadcast join

sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// Task 2: Explicitly Broadcast Join medals and maps
// Define required case classes that represent schemas for the data

case class Matches (
    match_id: Option[String],
    mapid: String,
    is_team_game: Boolean,
    playlist_id: String,
    game_variant_id: String,
    is_match_over: Boolean,
    completion_date: String,
    match_duration: String,
    game_mode: String,
    map_variant_id: String
)

case class MatchDetails (
    match_id: Option[String],
    player_gamertag: String,
    previous_spartan_rank: Integer,
    spartan_rank: Integer,
    previous_total_xp: Integer,
    total_xp: Integer,
    previous_csr_tier: Integer,
    previous_csr_designation: Integer,
    previous_csr: Integer,
    previous_csr_percent_to_next_tier: Integer,
    previous_csr_rank: Integer,
    current_csr_tier: Integer,
    current_csr_designation: Integer,
    current_csr: Integer,
    current_csr_percent_to_next_tier: Integer,
    current_csr_rank: Integer,
    player_rank_on_team: Integer,
    player_finished: Boolean,
    player_average_life: String,
    player_total_kills: Integer,
    player_total_headshots: Integer,
    player_total_weapon_damage: Double,
    player_total_shots_landed: Double,
    player_total_melee_kills: Double,
    player_total_melee_damage: Double,
    player_total_assassinations: Double,
    player_total_ground_pound_kills: Double,
    player_total_shoulder_bash_kills: Double,
    player_total_grenade_damage: Double,
    player_total_power_weapon_damage: Double,
    player_total_power_weapon_grabs: Double,
    player_total_deaths: Double,
    player_total_assists: Double,
    player_total_grenade_kills: Double,
    did_win: Double,
    team_id: Double
)

case class Maps (
    mapid: Option[String],
    name: String,
    description: String
)

case class Medals (
    medal_id: Option[Long], // Updated to Long
    sprite_uri: String,
    sprite_left: Integer,
    sprite_top: Integer,
    sprite_sheet_width: Integer,
    sprite_sheet_height: Integer,
    sprite_width: Integer,
    sprite_height: Integer,
    classification: String,
    description: String,
    name: String,
    difficulty: Integer
)

case class MedalsMatchesPlayers (
    match_id: Option[String],
    player_gamertag: String,
    medal_id: Long,
    count: Integer
)

// Read all required .csv files into datasets

val matches: Dataset[Matches] = sparkSession.read.option("header", "true")
                                .option("inferSchema", "true")
                                .csv("/home/iceberg/data/matches.csv")
                                .as[Matches]

//matches.show(2)

val matchDetails: Dataset[MatchDetails] = sparkSession.read.option("header", "true")
                                .option("inferSchema", "true")
                                .csv("/home/iceberg/data/match_details.csv")
                                .as[MatchDetails]

//matchDetails.show(2)

val maps: Dataset[Maps] = sparkSession.read.option("header", "true")
                                .option("inferSchema", "true")
                                .csv("/home/iceberg/data/maps.csv")
                                .as[Maps]
//maps.show(2)

val medals: Dataset[Medals] = sparkSession.read.option("header", "true")
                                .option("inferSchema", "true")
                                .csv("/home/iceberg/data/medals.csv")
                                .as[Medals]
//medals.show(2)

val medalsMatchesPlayers: Dataset[MedalsMatchesPlayers] = sparkSession.read.option("header", "true")
                                .option("inferSchema", "true")
                                .csv("/home/iceberg/data/medals_matches_players.csv")
                                .as[MedalsMatchesPlayers]
//medalsMatchesPlayers.show(2)                            

// Create Temporary Views for the required datasets

matches.createOrReplaceTempView("matchesView")

matchDetails.createOrReplaceTempView("matchDetailsView")

maps.createOrReplaceTempView("mapsView")

medalsMatchesPlayers.createOrReplaceTempView("medalsMatchesPlayersView")

medals.createOrReplaceTempView("medalsView")

// Create the long table (medals) using sql

val matchesMedalMapsAgg = sparkSession.sql("""
                                            SELECT mmp.medal_id,
                                                    me.name medal_name,
                                                    mmp.match_id,
                                                    ma.mapid,
                                                    COLLECT_LIST(DISTINCT mmp.player_gamertag) as player_gamertag_array
                                                    
                                            FROM medalsMatchesPlayersView mmp
                                            
                                            JOIN matchesView ma
                                            ON mmp.match_id = ma.match_id
                                            
                                            JOIN medalsView me
                                            ON mmp.medal_id = me.medal_id
                                        
                                            GROUP BY 1,2,3,4
                                        """) //.cache()
matchesMedalMapsAgg.show(5)

// Explicitly broadcast join Medals & Maps (Below is a Dataframe. It doesn't conform to a case class)

val medalsMaps = matchesMedalMapsAgg.as("m")
                    .join(broadcast(maps).as("mp"), $"m.mapid" === $"mp.mapid")
                    .select($"m.*", $"mp.name".as("map_name"), $"mp.description".as("map_description"))

medalsMaps.show(5)

// Task 3: Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets

// Create DDL for bucketed tables

// Matches
sparkSession.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

val bucketedMatchesDDL = """
                            CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
                                match_id STRING,
                                is_team_game BOOLEAN,
                                playlist_id STRING,
                                mapid STRING
                            )
                            USING iceberg
                            PARTITIONED BY (bucket(16, match_id));
                            """
sparkSession.sql(bucketedMatchesDDL)



// Match Details
sparkSession.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

val bucketedMatchDetailsDDL = """
                                CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
                                    match_id STRING,
                                    player_gamertag STRING,
                                    player_total_kills INTEGER,
                                    player_total_deaths INTEGER
                                )
                                USING iceberg
                                PARTITIONED BY (bucket(16, match_id));
                                """
sparkSession.sql(bucketedMatchDetailsDDL)



// Medals Matches Players
sparkSession.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")

val bucketedMedalMatchesPlayersDDL = """
                                        CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
                                            match_id STRING,
                                            player_gamertag STRING,
                                            medal_id BIGINT
                                        )
                                        USING iceberg
                                        PARTITIONED BY (bucket(16, match_id));
                                        """
sparkSession.sql(bucketedMedalMatchesPlayersDDL)

// Write data from the above datasets to the corresponding bucketed tables

// Matches
matches.select($"match_id", $"is_team_game", $"playlist_id", $"mapid")
        .write.mode("append")
        .bucketBy(16, "match_id")
        .saveAsTable("bootcamp.matches_bucketed")

// Match Details
matchDetails.select($"match_id", $"player_gamertag", $"player_total_kills", $"player_total_deaths")
            .write.mode("append")
            .bucketBy(16, "match_id")
            .saveAsTable("bootcamp.match_details_bucketed")

// Medals Matches Players
medalsMatchesPlayers.select($"match_id", $"player_gamertag", $"medal_id")
                    .write.mode("append")
                    .bucketBy(16, "match_id")
                    .saveAsTable("bootcamp.medal_matches_players_bucketed")

val bucketedTest = sparkSession.sql("""
                                                SELECT *
                                                       
                                                FROM bootcamp.medal_matches_players_bucketed mdb
                                                """)

bucketedTest.show(5)

// Bucket Join match_details, matches, and medal_matches_players

val bucketedMatchesMedals = sparkSession.sql("""
                                                SELECT mb.match_id,
                                                        mb.playlist_id,
                                                        mb.mapid,
                                                        mdb.player_gamertag,
                                                        mdb.player_total_kills,
                                                        mdb.player_total_deaths,
                                                        mmpb.medal_id
                                                       
                                                FROM bootcamp.matches_bucketed mb 
                                                            
                                                JOIN bootcamp.match_details_bucketed mdb 
                                                ON mb.match_id = mdb.match_id
                                                            
                                                JOIN bootcamp.medal_matches_players_bucketed mmpb
                                                ON mb.match_id = mmpb.match_id
                                
                                                WHERE mdb.player_gamertag IS NOT NULL
                                                """)

bucketedMatchesMedals.show(5)

// Join match_details, matches, and medal_matches_players temporary views (To see the difference between the one bucket joined)

sparkSession.sql("""
                SELECT mb.match_id,
                        mb.playlist_id,
                        mb.mapid,
                        mdb.player_gamertag,
                        mdb.player_total_kills,
                        mdb.player_total_deaths,
                        mmpb.medal_id
                
                FROM matchesView mb
                
                JOIN matchDetailsView mdb 
                ON mb.match_id = mdb.match_id
                
                JOIN medalsMatchesPlayersView mmpb
                ON mb.match_id = mmpb.match_id

                WHERE mdb.player_gamertag IS NOT NULL
                -- AND mb.completion_date = DATE('2016-01-01') 
                """).explain()

// Task 4: Aggregate the joined data frame to figure out questions like:

// Saved the bucketed table as a temporary view

bucketedMatchesMedals.createOrReplaceTempView("bucketed_matches_medals")

// I. Which player averages the most kills per game?

val playersAvgKills = sparkSession.sql("""
                                        SELECT player_gamertag, 
                                               AVG(player_total_kills) average_kills 
                                        
                                        FROM bucketed_matches_medals
                                        GROUP BY player_gamertag
                                        ORDER BY average_kills DESC
                                        """)

playersAvgKills.show(1)

// II. Which playlist gets played the most?

val playlistMostPlayed = sparkSession.sql("""
                                        SELECT playlist_id, 
                                               COUNT(match_id) playlist_plays 
                                        
                                        FROM bucketed_matches_medals
                                        GROUP BY playlist_id
                                        ORDER BY playlist_plays DESC
                                        """)

playlistMostPlayed.show(1)

// III. Which map gets played the most?

val mapMostPlayed = sparkSession.sql("""
                                        SELECT map.name map, 
                                               COUNT(match.match_id) map_plays 
                                        
                                        FROM bucketed_matches_medals match
                                        JOIN mapsView map
                                        on match.mapid = map.mapid
                                        GROUP BY map.name
                                        ORDER BY map_plays DESC
                                        """)

mapMostPlayed.show(1)

// IV. Which map do players get the most Killing Spree medals on?

val mapMostKillingMedals = sparkSession.sql("""
                                            SELECT map.name map, 
                                               SUM(match.player_total_kills) map_player_killings 
                                        
                                            FROM bucketed_matches_medals match
                                            JOIN mapsView map
                                            on match.mapid = map.mapid
                                            GROUP BY map.name
                                            ORDER BY map_player_killings DESC
                                            """)

mapMostKillingMedals.show(1)

// Task 5: With the aggregated data set, try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
// I. playlistMostPlayed

val sortPartitionPlaylistMostPlayed = playlistMostPlayed.repartition(col("playlist_id")).sortWithinPartitions(col("playlist_plays").desc)

sortPartitionPlaylistMostPlayed.show(1)

// II. mapMostPlayed

val sortPartitionPMapMostPlayed = mapMostPlayed.repartition(col("map")).sortWithinPartitions(col("map_plays").desc)

sortPartitionPMapMostPlayed.show(1)

// III. mapMostKillingMedals

val sortPartitionPMapMostKillingMedals = mapMostKillingMedals.repartition(col("map")).sortWithinPartitions(col("map_player_killings").desc)

sortPartitionPMapMostKillingMedals.show(1)
