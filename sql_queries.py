songplays_query = ("""
select a.timestamp as start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent, year(a.timestamp) as year, month(a.timestamp) as month 
from staging_logs as a 
inner join songs as b on a.song = b.song_title
""")