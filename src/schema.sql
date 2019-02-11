BEGIN;

CREATE TABLE IF NOT EXISTS play (
       id INTEGER PRIMARY KEY,
       time INTEGER NOT NULL,
       track_mbid VARCHAR(36),
       track_name TEXT,
       artist_mbid VARCHAR(36),
       artist_name TEXT,
       album_mbid VARCHAR(36),
       album_name TEXT
);

CREATE INDEX IF NOT EXISTS index_track_mbid ON play(track_mbid);
CREATE INDEX IF NOT EXISTS index_artist_mbid ON play(artist_mbid);
CREATE INDEX IF NOT EXISTS index_album_mbid ON play(album_mbid);

COMMIT;
