import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = """DROP TABLE IF EXISTS staging_events"""
staging_songs_table_drop = """DROP TABLE IF EXISTS staging_songs"""
songplay_table_drop = """DROP TABLE IF EXISTS songplay"""
user_table_drop = """DROP TABLE IF EXISTS users"""
song_table_drop = """DROP TABLE IF EXISTS songs"""
artist_table_drop = """DROP TABLE IF EXISTS artists"""
time_table_drop = """DROP TABLE IF EXISTS time"""

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXIST staging_events(
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender VARCHAR(1),
        item_session INTEGER,
        last_name text,
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

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_song INTEGER,
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

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay(
        songplay_id INT IDENTITY(1,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INTEGER NOT NULL,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender CHAR(1),
        level TEXT
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id INTEGER PRIMARY KEY,
        title TEXT,
        artist_id TEXT,
        year INTEGER,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
        (artist_id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude NUMERIC,
        longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
        (start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        weekday INTEGER
    )
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events
        from {}
        iam_role {}
        json {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
        from {}
        iam_role {}
        json 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) as user_id
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id is NOT NULL
    and page == 'NextSong'
    """)

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) as song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id IS NO NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time) as start_time,
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        EXTRACT(weekday FROM start_time) as weekday
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
