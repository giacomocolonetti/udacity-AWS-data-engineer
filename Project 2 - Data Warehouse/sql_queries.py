import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

data_warehouse_arn = config.get("IAM_ROLE", "ARN")

s3_log_data = config.get("S3", "LOG_DATA")
s3_log_jsonpath = config.get("S3", "LOG_JSONPATH")
s3_song_data = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          DOUBLE PRECISION,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR,
    user_id         INTEGER
) DISTSTYLE EVEN;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs        INTEGER NOT NULL,
    artist_id        VARCHAR NOT NULL,
    artist_latitude  VARCHAR,
    artist_longitude VARCHAR,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR NOT NULL,
    title            VARCHAR,
    duration         DOUBLE PRECISION,
    year             INTEGER
) DISTSTYLE EVEN;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(0,1) SORTKEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     VARCHAR(50) NOT NULL,
    song_id     VARCHAR(50) NOT NULL,
    artist_id   VARCHAR(50) NOT NULL,
    session_id  INTEGER NOT NULL,
    location    VARCHAR(250) NOT NULL,
    user_agent  VARCHAR(250) NOT NULL
) DISTSTYLE EVEN;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER NOT NULL SORTKEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(100),
    gender     VARCHAR(1),
    level      VARCHAR(10)
) DISTSTYLE EVEN;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR(50) NOT NULL SORTKEY,
    title     VARCHAR(250),
    artist_id VARCHAR(50) NOT NULL,
    duration  DOUBLE PRECISION,
    year      SMALLINT
) DISTSTYLE EVEN;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(250) NOT NULL SORTKEY,
    name      VARCHAR(250),
    latitude  VARCHAR(250),
    longitude VARCHAR(250),
    location  VARCHAR(250)
) DISTSTYLE EVEN;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour       SMALLINT NOT NULL,
    day        SMALLINT NOT NULL,
    week       SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    year       SMALLINT NOT NULL,
    weekday    VARCHAR(10) NOT NULL
) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events FROM '{s3_log_data}'
CREDENTIALS 'aws_iam_role={data_warehouse_arn}'
FORMAT AS JSON '{s3_log_jsonpath}'
REGION 'us-west-2'
""")

staging_songs_copy = (f"""
COPY staging_songs FROM '{s3_song_data}'
CREDENTIALS 'aws_iam_role={data_warehouse_arn}'
FORMAT AS JSON 'auto'
REGION 'us-west-2'
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
SELECT 
    DISTINCT(TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') as start_time,
    se.user_id       AS user_id,
    ss.song_id       AS song_id,
    ss.artist_id     AS artist_id,
    se.session_id    AS session_id,
    se.location      AS location,
    se.user_agent    AS user_agent
FROM staging_events AS se
JOIN staging_songs AS ss ON (se.song = ss.title)
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT (user_id) AS user_id,
    first_name         AS first_name,
    last_name          AS last_name,
    gender             AS gender,
    level              AS level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, duration, year)
SELECT
    DISTINCT(song_id) AS song_id,
    title             AS title,
    artist_id         AS artist_id,
    duration          AS duration,
    year              AS year
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, latitude, longitude, location)
SELECT
    DISTINCT(artist_id) AS artist_id,
    artist_name         AS name,
    artist_latitude     AS latitude,
    artist_longitude    AS longitude,
    artist_location     AS location
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') as start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
