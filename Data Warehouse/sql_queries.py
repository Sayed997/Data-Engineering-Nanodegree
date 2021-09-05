import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplays"
user_table_drop = "DROP TABLE IF EXISTS dimUsers"
song_table_drop = "DROP TABLE IF EXISTs dimSongs"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

#Here the sequence of the file headers were followed so COPY can correctly map the columns
#We will try and build an efficient table design that avoids too much shuffling
#staging tables will not need a sort key as they won't be queried
#thinking about what ids will be used for joins I decided to go with the following Distkeys for efficient data distribtion
#Data tables were sorted keeping the order by query in mind

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    events_id        BIGINT IDENTITY(0,1),
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           VARCHAR,
    itemInSession    INTEGER,
    lastName         VARCHAR,
    length           DECIMAL,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     DECIMAL,
    sessionId        INTEGER,
    song             VARCHAR,
    status           INTEGER,
    ts               BIGINT,
    userAgent        VARCHAR,
    userId           INTEGER)
    distkey(artist);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INTEGER)
    distkey(artist_name);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factSongplays (
    songplay_id    INTEGER IDENTITY(0,1)   NOT NULL,
    start_time     TIMESTAMP               NOT NULL,
    user_id        INTEGER,
    level          VARCHAR(255),
    song_id        VARCHAR(255),
    artist_id      VARCHAR(MAX),
    session_id     INTEGER,
    location       VARCHAR(255),
    user_agent     VARCHAR,
    PRIMARY KEY(songplay_id))
    diststyle all
    sortkey(song_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimUsers (
    user_id       INTEGER                  NOT NULL,
    first_name    VARCHAR(255),
    last_name     VARCHAR(255),
    gender        VARCHAR(50),
    level         VARCHAR(50),
    PRIMARY KEY(user_id))
    diststyle all
    sortkey(last_name);
""") 

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimSongs (
    song_id       VARCHAR(255)              NOT NULL,
    title         VARCHAR(255),
    artist_id     VARCHAR(MAX),
    year          INTEGER,
    duration      DECIMAL,
    PRIMARY KEY(song_id))
    distkey(song_id)
    sortkey(year);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimArtists (
    artist_id     VARCHAR(MAX)              NOT NULL,
    name          VARCHAR(255),
    location      VARCHAR(MAX),
    latitude      DECIMAL,
    longitude     DECIMAL,
    PRIMARY KEY(artist_id))
    distkey(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimTime (
    start_time    TIMESTAMP               NOT NULL,
    hour          SMALLINT,
    day           SMALLINT,
    week          SMALLINT,
    month         SMALLINT,
    year          SMALLINT,
    weekday       SMALLINT,
    PRIMARY KEY(start_time))
    distkey(start_time)
    sortkey(start_time); 
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} 
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' 
    ACCEPTINVCHARS as '^'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
#Insert songplay table where page ="NextSong"
songplay_table_insert = ("""
    INSERT INTO 
    factSongplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO
    dimUsers(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
    se.userId AS user_id,
    se.firstName AS first_name,
    se.lastName AS last_name,
    se.gender AS gender,
    se.level AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO 
    dimSongs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
    ss.song_id AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO 
    dimArtists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
    ss.artist_id AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO
    dimTime(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time)  AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time)  AS year,
    EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
