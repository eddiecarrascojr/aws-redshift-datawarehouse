import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('dwh.cfg')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist TEXT,
                                auth TEXT,
                                first_name TEXT,
                                gender CHAR(1),
                                item_session INTEGER,
                                last_name TEXT,
                                length NUMERIC,
                                level TEXT,
                                location TEXT,
                                method TEXT,
                                page TEXT,
                                registration NUMERIC,
                                session_id INTEGER,
                                song TEXT,
                                status INTEGER,
                                ts BIGINT,
                                user_agent TEXT,
                                user_id INTEGER
                            )
""")

staging_songs_table_create = ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                num_songs INTEGER,
                                artist_id TEXT,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id TEXT,
                                title TEXT,
                                duration NUMERIC,
                                year INTEGER
                            )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INT IDENTITY(1,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id INTEGER NOT NULL,
                            level TEXT,
                            song_id TEXT,
                            artist_id TEXT,
                            session_id INTEGER,
                            location TEXT,
                            user_agent TEXT
                        )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY,
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        gender CHAR(1),
                        level TEXT
                    )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id TEXT PRIMARY KEY,
                        title TEXT,
                        artist_id TEXT,
                        year INTEGER,
                        duration NUMERIC
                    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id TEXT PRIMARY KEY,
                          name TEXT,
                          location TEXT,
                          latitude NUMERIC,
                          longitude NUMERIC
                       )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekDay INTEGER
                    )
""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(LOG_DATA, str(IAM_ROLE), LOG_JSONPATH)

staging_songs_copy = ("""
            copy staging_songs from {data_bucket}
            credentials 'aws_iam_role={role_arn}'
            region 'us-west-2' format as JSON 'auto';
            """).format(data_bucket=SONG_DATA, role_arn=str(IAM_ROLE))

# FINAL TABLES
user_table_insert = """INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct  user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'; """

song_table_insert = """INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
"""

artist_table_insert = """INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
"""

songplay_table_insert = """INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.user_id as user_id, se.level,
                                    ss.song_id, ss.artist_id, se.session_id as session_id, se.location, se.user_agent as user_agent
                            FROM staging_events se, staging_songs ss
                            WHERE se.page = 'NextSong' AND
                            se.song = ss.title AND
                            se.artist = ss.artist_name AND
                            se.length = ss.duration
"""

time_table_insert = """INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
                        SELECT start_time, extract(hour from start_time), extract(day from start_time),
                                extract(week from start_time), extract(month from start_time),
                                extract(year from start_time), extract(dayofweek from start_time)
                        FROM songplay
"""

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
select_number_rows_queries = [get_number_staging_events, get_number_staging_songs, get_number_songplays,
                              get_number_users, get_number_songs, get_number_artists, get_number_time]