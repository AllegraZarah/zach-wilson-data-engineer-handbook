#!/usr/bin/env python
# coding: utf-8

# In[23]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, split, lit, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, LongType

# Initialize a Spark Session
spark = SparkSession.builder.appName("Assignment3").getOrCreate()


# In[2]:


# Task 1: Disable automatic broadcast join

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


# In[4]:


# Task 2: Explicitly Broadcast Join medals and maps
# Define required case classes that represent schemas for the data

# Define schema for Matches
matches_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("mapid", StringType(), True),
    StructField("is_team_game", BooleanType(), True),
    StructField("playlist_id", StringType(), True),
    StructField("game_variant_id", StringType(), True),
    StructField("is_match_over", BooleanType(), True),
    StructField("completion_date", StringType(), True),
    StructField("match_duration", StringType(), True),
    StructField("game_mode", StringType(), True),
    StructField("map_variant_id", StringType(), True)
])

# Define schema for MatchDetails
match_details_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("player_gamertag", StringType(), True),
    StructField("previous_spartan_rank", IntegerType(), True),
    StructField("spartan_rank", IntegerType(), True),
    StructField("previous_total_xp", IntegerType(), True),
    StructField("total_xp", IntegerType(), True),
    StructField("previous_csr_tier", IntegerType(), True),
    StructField("previous_csr_designation", IntegerType(), True),
    StructField("previous_csr", IntegerType(), True),
    StructField("previous_csr_percent_to_next_tier", IntegerType(), True),
    StructField("previous_csr_rank", IntegerType(), True),
    StructField("current_csr_tier", IntegerType(), True),
    StructField("current_csr_designation", IntegerType(), True),
    StructField("current_csr", IntegerType(), True),
    StructField("current_csr_percent_to_next_tier", IntegerType(), True),
    StructField("current_csr_rank", IntegerType(), True),
    StructField("player_rank_on_team", IntegerType(), True),
    StructField("player_finished", BooleanType(), True),
    StructField("player_average_life", StringType(), True),
    StructField("player_total_kills", IntegerType(), True),
    StructField("player_total_headshots", IntegerType(), True),
    StructField("player_total_weapon_damage", DoubleType(), True),
    StructField("player_total_shots_landed", DoubleType(), True),
    StructField("player_total_melee_kills", DoubleType(), True),
    StructField("player_total_melee_damage", DoubleType(), True),
    StructField("player_total_assassinations", DoubleType(), True),
    StructField("player_total_ground_pound_kills", DoubleType(), True),
    StructField("player_total_shoulder_bash_kills", DoubleType(), True),
    StructField("player_total_grenade_damage", DoubleType(), True),
    StructField("player_total_power_weapon_damage", DoubleType(), True),
    StructField("player_total_power_weapon_grabs", DoubleType(), True),
    StructField("player_total_deaths", DoubleType(), True),
    StructField("player_total_assists", DoubleType(), True),
    StructField("player_total_grenade_kills", DoubleType(), True),
    StructField("did_win", DoubleType(), True),
    StructField("team_id", DoubleType(), True)
])

# Define schema for Maps
maps_schema = StructType([
    StructField("mapid", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True)
])

# Define schema for Medals
medals_schema = StructType([
    StructField("medal_id", LongType(), True),
    StructField("sprite_uri", StringType(), True),
    StructField("sprite_left", IntegerType(), True),
    StructField("sprite_top", IntegerType(), True),
    StructField("sprite_sheet_width", IntegerType(), True),
    StructField("sprite_sheet_height", IntegerType(), True),
    StructField("sprite_width", IntegerType(), True),
    StructField("sprite_height", IntegerType(), True),
    StructField("classification", StringType(), True),
    StructField("description", StringType(), True),
    StructField("name", StringType(), True),
    StructField("difficulty", IntegerType(), True)
])

# Define schema for MedalsMatchesPlayers
medals_matches_players_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("player_gamertag", StringType(), True),
    StructField("medal_id", LongType(), True),
    StructField("count", IntegerType(), True)
])


# In[5]:


# Read all required .csv files into DataFrames
matches = spark.read.option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv("/home/iceberg/data/matches.csv")

# Show the first 2 rows
matches.show(2)


# In[6]:


match_details = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv("/home/iceberg/data/match_details.csv")

# Show the first 2 rows
match_details.show(2)


# In[7]:


maps = spark.read.option("header", "true") \
                 .option("inferSchema", "true") \
                 .csv("/home/iceberg/data/maps.csv")

# Show the first 2 rows
maps.show(2)


# In[8]:


medals = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv("/home/iceberg/data/medals.csv")

# Show the first 2 rows
medals.show(2)


# In[9]:


medals_matches_players = spark.read.option("header", "true") \
                                   .option("inferSchema", "true") \
                                   .csv("/home/iceberg/data/medals_matches_players.csv")

# Show the first 2 rows
medals_matches_players.show(2)


# In[10]:


# Create Temporary Views for the required DataFrames

matches.createOrReplaceTempView("matchesView")

match_details.createOrReplaceTempView("matchDetailsView")

maps.createOrReplaceTempView("mapsView")

medals_matches_players.createOrReplaceTempView("medalsMatchesPlayersView")

medals.createOrReplaceTempView("medalsView")


# In[11]:


# Create the long table (medals) using SQL

matches_medal_maps_agg = spark.sql("""
    SELECT mmp.medal_id,
           me.name AS medal_name,
           mmp.match_id,
           ma.mapid,
           COLLECT_LIST(DISTINCT mmp.player_gamertag) AS player_gamertag_array
    FROM medalsMatchesPlayersView mmp
    JOIN matchesView ma ON mmp.match_id = ma.match_id
    JOIN medalsView me ON mmp.medal_id = me.medal_id
    GROUP BY mmp.medal_id, me.name, mmp.match_id, ma.mapid
""")

# Show the first 5 rows
matches_medal_maps_agg.show(5)


# In[12]:


# Explicitly broadcast join Medals & Maps

medals_maps = matches_medal_maps_agg.alias("m") \
    .join(broadcast(maps).alias("mp"), matches_medal_maps_agg["mapid"] == maps["mapid"]) \
    .select("m.*", "mp.name", "mp.description")

# Show the first 5 rows
medals_maps.show(5)


# In[13]:


# Task 3: Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets

# Create DDL for bucketed tables

# Matches
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

bucketed_matches_ddl = """
                        CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
                            match_id STRING,
                            is_team_game BOOLEAN,
                            playlist_id STRING,
                            mapid STRING
                        )
                        USING iceberg
                        PARTITIONED BY (bucket(16, match_id))
                        """
spark.sql(bucketed_matches_ddl)



# Match Details
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

bucketed_match_details_ddl = """
                                CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
                                    match_id STRING,
                                    player_gamertag STRING,
                                    player_total_kills INTEGER,
                                    player_total_deaths INTEGER
                                )
                                USING iceberg
                                PARTITIONED BY (bucket(16, match_id))
                                """
spark.sql(bucketed_match_details_ddl)



# Medals Matches Players
spark.sql("""DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed""")

bucketed_medal_matches_players_ddl = """
                                        CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
                                            match_id STRING,
                                            player_gamertag STRING,
                                            medal_id BIGINT
                                        )
                                        USING iceberg
                                        PARTITIONED BY (bucket(16, match_id))
                                        """
spark.sql(bucketed_medal_matches_players_ddl)


# In[14]:


# Write data from the above DataFrames to the corresponding bucketed tables

# Matches
matches.select("match_id", "is_team_game", "playlist_id", "mapid") \
       .write.mode("append") \
       .bucketBy(16, "match_id") \
       .saveAsTable("bootcamp.matches_bucketed")

# Match Details
match_details.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
             .write.mode("append") \
             .bucketBy(16, "match_id") \
             .saveAsTable("bootcamp.match_details_bucketed")

# Medals Matches Players
medals_matches_players.select("match_id", "player_gamertag", "medal_id") \
                      .write.mode("append") \
                      .bucketBy(16, "match_id") \
                      .saveAsTable("bootcamp.medal_matches_players_bucketed")


# In[17]:


# Bucket Join match_details, matches, and medal_matches_players

bucketed_matches_medals = spark.sql("""
                                    SELECT mb.match_id,
                                           mb.playlist_id,
                                           mb.mapid,
                                           mdb.player_gamertag,
                                           mdb.player_total_kills,
                                           mdb.player_total_deaths,
                                           mmpb.medal_id
                                    FROM bootcamp.matches_bucketed mb
                                    JOIN bootcamp.match_details_bucketed mdb ON mb.match_id = mdb.match_id
                                    JOIN bootcamp.medal_matches_players_bucketed mmpb ON mb.match_id = mmpb.match_id
                                    WHERE mdb.player_gamertag IS NOT NULL
                                """)

# Show the first 5 rows
bucketed_matches_medals.show(5)


# In[18]:


# Task 4: Aggregate the joined data frame to figure out questions like:

# Save the bucketed table as a temporary view

bucketed_matches_medals.createOrReplaceTempView("bucketed_matches_medals")


# In[19]:


# I. Which player averages the most kills per game?

players_avg_kills = spark.sql("""
                                SELECT player_gamertag, 
                                       AVG(player_total_kills) AS average_kills 
                                FROM bucketed_matches_medals
                                GROUP BY player_gamertag
                                ORDER BY average_kills DESC
                            """)

players_avg_kills.show(1)


# In[20]:


# II. Which playlist gets played the most?

playlist_most_played = spark.sql("""
                                    SELECT playlist_id, 
                                           COUNT(match_id) AS playlist_plays 
                                    FROM bucketed_matches_medals
                                    GROUP BY playlist_id
                                    ORDER BY playlist_plays DESC
                                """)

playlist_most_played.show(1)


# In[21]:


# III. Which map gets played the most?

map_most_played = spark.sql("""
                            SELECT map.name AS map, 
                                   COUNT(match.match_id) AS map_plays 
                            FROM bucketed_matches_medals match
                            JOIN mapsView map ON match.mapid = map.mapid
                            GROUP BY map.name
                            ORDER BY map_plays DESC
                        """)

map_most_played.show(1)


# In[28]:


# IV. Which map do players get the most Killing Spree medals on?

map_most_killing_spree_medals = spark.sql("""
                                        SELECT map.name AS map, 
                                               COUNT(match.medal_id) AS killing_spree_medals 
                                        FROM bucketed_matches_medals match
                                        JOIN mapsView map ON match.mapid = map.mapid
                                        JOIN medalsView med ON match.medal_id = med.medal_id

                                        WHERE med.name = 'Killing Spree'
                                        
                                        GROUP BY map.name
                                        ORDER BY killing_spree_medals DESC
                                    """)

map_most_killing_spree_medals.show(1)


# In[24]:


# Task 5: With the aggregated data set, try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

# I. playlistMostPlayed

sort_partition_playlist_most_played = playlist_most_played.repartition(col("playlist_id")) \
                                                          .sortWithinPartitions(col("playlist_plays").desc())

sort_partition_playlist_most_played.show(1)


# In[25]:


# II. mapMostPlayed

sort_partition_map_most_played = map_most_played.repartition(col("map")) \
                                                .sortWithinPartitions(col("map_plays").desc())

sort_partition_map_most_played.show(1)


# In[30]:


# III. mapMostKillingMedals

sort_partition_map_most_killing_spree_medals = map_most_killing_spree_medals.repartition(col("map")) \
                                                                .sortWithinPartitions(col("killing_spree_medals").desc())

sort_partition_map_most_killing_spree_medals.show(1)


# In[ ]:




