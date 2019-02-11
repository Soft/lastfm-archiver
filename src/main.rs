use std::path::PathBuf;
use std::process;
use std::str::FromStr;

use chrono::NaiveDateTime;
use failure::{err_msg, Error};
use hyper::client::connect::Connect;
use hyper::{Client, Request, Body};
use hyper_tls::HttpsConnector;
use rusqlite::Connection;
use rusqlite::types::ToSql;
use structopt::StructOpt;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use urlencoding::encode;
use xmltree::Element;

const DOMAIN: &str = "https://ws.audioscrobbler.com";
const MAX_TRACKS: u64 = 200;

#[derive(StructOpt)]
#[structopt(name = "lastfm-archiver", about = "Archive last.fm listening history.")]
struct Command {
    #[structopt(help = "API Key")]
    api_key: String,
    #[structopt(help = "Username")]
    user: String,
    #[structopt(help = "Database path")]
    database: PathBuf,
}

#[derive(Debug)]
struct Artist {
    mbid: Option<String>,
    name: String,
}

#[derive(Debug)]
struct Album {
    mbid: Option<String>,
    name: String,
}

#[derive(Debug)]
struct Track {
    artist: Option<Artist>,
    album: Option<Album>,
    mbid: Option<String>,
    name: String,
    time: NaiveDateTime,
}

impl Track {
    fn insert(&self, connection: &Connection) -> Result<(), Error> {
        connection
            .execute(
                r#"
          INSERT INTO play (
            time, track_mbid, track_name, artist_mbid, artist_name, album_mbid, album_name
          ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7
          )"#,
                &[
                    &self.time.timestamp() as &ToSql,
                    &self.mbid as &ToSql,
                    &self.name as &ToSql,
                    &self.artist.as_ref().and_then(|artist| artist.mbid.as_ref()) as &ToSql,
                    &self.artist.as_ref().map(|artist| &artist.name) as &ToSql,
                    &self.album.as_ref().and_then(|album| album.mbid.as_ref()) as &ToSql,
                    &self.album.as_ref().map(|album| &album.name) as &ToSql,
                ],
            )
            .map_err(From::from)
            .map(|_| ())
    }
}

impl Track {
    fn build_track(mut track: Element) -> Result<Track, Error> {
        let artist = track.take_child("artist").ok_or_else(
            || err_msg("missing artist"),
        )?;
        let artist = match artist.text {
            Some(name) => {
                let mbid = artist.attributes.get("mbid").cloned().filter(
                    |mbid| mbid != "",
                );
                Some(Artist { mbid, name })
            }
            None => None,
        };
        let album = track.take_child("album").ok_or_else(
            || err_msg("missing album"),
        )?;
        let album = match album.text {
            Some(name) => {
                let mbid = album.attributes.get("mbid").cloned().filter(
                    |mbid| mbid != "",
                );
                Some(Album { mbid, name })
            }
            None => None,
        };
        let mbid = track
            .take_child("mbid")
            .ok_or_else(|| err_msg("missing mbid"))?
            .text
            .filter(|mbid| mbid != "");
        let name = track
            .take_child("name")
            .ok_or_else(|| err_msg("missing name"))?
            .text
            .ok_or_else(|| err_msg("empty name"))?;
        let time = track
            .take_child("date")
            .ok_or_else(|| err_msg("missing date"))?
            .attributes
            .get("uts")
            .ok_or_else(|| err_msg("missing timestamp"))
            .and_then(|str| i64::from_str(str).map_err(From::from))
            .map(|secs| NaiveDateTime::from_timestamp(secs, 0))?;
        Ok(Track {
            artist,
            album,
            mbid,
            name,
            time,
        })
    }
}

#[derive(Debug)]
struct Response {
    page: u64,
    total_pages: u64,
    tracks: Vec<Track>,
}

impl Response {
    fn build_response(response: Element) -> Result<Response, Error> {
        let page = response
            .attributes
            .get("page")
            .ok_or_else(|| err_msg("missing page"))
            .and_then(|str| u64::from_str(str).map_err(From::from))?;
        let total_pages = response
            .attributes
            .get("totalPages")
            .ok_or_else(|| err_msg("missing totalPages"))
            .and_then(|str| u64::from_str(str).map_err(From::from))?;
        let tracks: Result<Vec<Track>, Error> = response
            .children
            .into_iter()
            .filter(move |track| {
                if let Some(status) = track.attributes.get("nowplaying") {
                    return status != "true";
                }
                return true;
            })
            .map(Track::build_track)
            .collect();
        let tracks = tracks?;
        Ok(Response {
            page,
            total_pages,
            tracks,
        })
    }

    fn from_slice<'a>(v: &'a [u8]) -> Result<Response, Error> {
        let mut root = Element::parse(v).map_err(Error::from)?;
        let status = root.attributes.get("status").ok_or_else(
            || err_msg("missing status"),
        )?;
        match status.as_ref() {
            "ok" => {
                Response::build_response(root.take_child("recenttracks").ok_or_else(|| {
                    err_msg("missing recenttracks")
                })?)
            }
            "failed" => {
                let error = root.take_child("error")
                    .ok_or_else(|| err_msg("missing error"))?
                    .text
                    .ok_or_else(|| err_msg("missing error message"))?;
                Err(err_msg(error))
            }
            _ => Err(err_msg("unknown status")),

        }
    }
}

fn fetch_tracks<T>(
    client: Client<T>,
    api_key: String,
    user: String,
) -> impl Stream<Item = Track, Error = Error>
where
    T: 'static + Sync + Connect,
{
    let user_agent = format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    stream::unfold(Some(1), move |next| {
        let next = match next {
            Some(n) => n,
            None => return None,
        };

        let url = format!(
            "{}/2.0/?method=user.getrecenttracks&limit={}&user={}&api_key={}&page={}",
            DOMAIN,
            MAX_TRACKS,
            encode(&user),
            encode(&api_key),
            next
        );
        let request = Request::get(url)
            .header("User-Agent", user_agent.clone())
            .body(Body::empty())
            .unwrap();
        Some(client.request(request).from_err::<Error>().and_then(
            move |response| {
                response
                    .into_body()
                    .concat2()
                    .from_err()
                    .and_then(move |body| Response::from_slice(&body))
                    .map(move |response| {
                        let next = if response.page < response.total_pages {
                            Some(response.page + 1)
                        } else {
                            None
                        };
                        (stream::iter_ok(response.tracks), next)
                    })
            },
        ))
    }).flatten()
}

fn setup_database(connection: &Connection) -> Result<(), Error> {
    connection
        .execute_batch(include_str!("schema.sql"))
        .map_err(From::from)
}

fn archiver<T>(
    client: Client<T>,
    api_key: String,
    user: String,
    connection: Connection,
) -> impl Future<Item = (), Error = Error>
where
    T: 'static + Sync + Connect,
{
    future::result(setup_database(&connection)).and_then(|_| {
        fetch_tracks(client, api_key, user).for_each(move |track| -> Result<(), Error> {
            track.insert(&connection)?;
            Ok(())
        })
    })
}

fn process() -> Result<(), Error> {
    let options = Command::from_args();
    let runtime = Runtime::new()?;
    let https = HttpsConnector::new(num_cpus::get())?;
    let client = Client::builder().build(https);
    let connection = Connection::open(options.database)?;
    let archiver = archiver(client, options.api_key, options.user, connection);
    runtime.block_on_all(archiver)
}

fn main() {
    if let Err(err) = process() {
        eprintln!("{}", err);
        process::exit(1);
    }
}
