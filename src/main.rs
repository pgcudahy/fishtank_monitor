use std::{
    net::ToSocketAddrs,
    num::{NonZeroU16, NonZeroU64},
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use native_tls::TlsConnector;
use polars::prelude::*;
use bytes::Bytes;
use futures_util::SinkExt;
use postgres_native_tls::MakeTlsConnector;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use tokio_postgres::Client;

struct Row {
    ts: i64, // Unix seconds
    sensor: String,
    value: f64,
}

#[derive(Deserialize)]
struct Payload {
    ts: i64, // Unix seconds, set by the sensor from its NTP clock
    value: f64,
}

// ESPHome publishes JSON {"ts": <unix seconds>, "value": <number>} to fishtank/reading/<name>
fn parse(topic: &str, payload: &[u8]) -> Result<Row, String> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() != 3 || parts[1] != "reading" || parts[2].is_empty() {
        return Err(format!("unexpected topic '{topic}'"));
    }
    let p: Payload = serde_json::from_slice(payload).map_err(|e| {
        format!("payload '{}' on '{topic}' is invalid: {e}", String::from_utf8_lossy(payload))
    })?;
    if !p.value.is_finite() {
        return Err(format!("value on '{topic}' is not finite"));
    }
    // Reject clocks that were never synced (near 1970) or are far in the future
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    if p.ts < 1_577_836_800 || p.ts > now + 86_400 {
        return Err(format!("timestamp {} on '{topic}' is out of range", p.ts));
    }
    Ok(Row { ts: p.ts, sensor: parts[2].to_string(), value: p.value })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database_url: String,
    pg_tls: bool,
    flush_secs: NonZeroU64,
    http_port: NonZeroU16,
    mqtt: MqttConfig,
    gotify: GotifyConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GotifyConfig {
    url: String,
    token: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MqttConfig {
    host: String,
    port: NonZeroU16,
    client_id: String,
    topic: String,
    // Optional: omit both for a broker that allows anonymous clients
    user: Option<String>,
    password: Option<String>,
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        let blank = |name: &str, v: &str| {
            if v.trim().is_empty() { Err(format!("'{name}' must not be empty")) } else { Ok(()) }
        };

        let pg: tokio_postgres::Config = self
            .database_url
            .parse()
            .map_err(|e| format!("'database_url' is malformed: {e}"))?;
        // An empty or host-less URL parses successfully, so check explicitly
        if pg.get_hosts().is_empty() {
            return Err("'database_url' must include a host".into());
        }
        let url: reqwest::Url = self
            .gotify
            .url
            .parse()
            .map_err(|e| format!("'gotify.url' is malformed: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
            return Err("'gotify.url' must be an http(s) URL with a host".into());
        }
        blank("gotify.token", &self.gotify.token)?;
        let host = self.mqtt.host.as_str();
        (host, self.mqtt.port.get())
            .to_socket_addrs()
            .map_err(|e| format!("'mqtt.host' '{host}' cannot be resolved: {e}"))?;
        blank("mqtt.client_id", &self.mqtt.client_id)?;
        blank("mqtt.topic", &self.mqtt.topic)?;
        if self.mqtt.user.is_some() != self.mqtt.password.is_some() {
            return Err("'mqtt.user' and 'mqtt.password' must be set together".into());
        }
        Ok(())
    }
}

struct Notifier {
    http: reqwest::Client,
    url: String,
    token: String,
}

impl Notifier {
    fn new(cfg: GotifyConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("http client");
        let url = format!("{}/message", cfg.url.trim_end_matches('/'));
        Self { http, url, token: cfg.token }
    }

    // Best effort: a failed notification is only logged, never fatal
    async fn send(&self, title: &str, message: &str, priority: u8) {
        let res = self
            .http
            .post(&self.url)
            .header("X-Gotify-Key", &self.token)
            .json(&serde_json::json!({ "title": title, "message": message, "priority": priority }))
            .send()
            .await
            .and_then(|r| r.error_for_status());
        if let Err(e) = res {
            eprintln!("gotify notification failed: {e}");
        }
    }
}

fn load_config() -> Result<Config, String> {
    let path = env::args().nth(1).unwrap_or_else(|| "config.toml".into());
    let text = std::fs::read_to_string(&path).map_err(|e| format!("cannot read '{path}': {e}"))?;
    let cfg: Config = toml::from_str(&text).map_err(|e| format!("invalid config '{path}':\n{e}"))?;
    cfg.validate().map_err(|e| format!("invalid config '{path}': {e}"))?;
    Ok(cfg)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let cfg = load_config().unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let db_url = cfg.database_url;
    let pg_tls = cfg.pg_tls;
    let notifier = Notifier::new(cfg.gotify);
    if let Err(e) = connect(&db_url, pg_tls).await {
        // tokio_postgres::Error's Display is just "db error"; the cause is in its source
        let cause = std::error::Error::source(&*e).map(|c| format!(": {c}")).unwrap_or_default();
        eprintln!("error: cannot connect to database: {e}{cause}");
        notifier.send("Fishtank monitor stopped", &format!("Cannot connect to database: {e}{cause}"), 8).await;
        std::process::exit(1);
    }
    println!("database connection ok");

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", cfg.http_port.get()))
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: cannot bind HTTP port {}: {e}", cfg.http_port);
            std::process::exit(1);
        });
    let app = axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("http server stopped: {e}");
        }
    });

    let m = cfg.mqtt;
    let mut opts = MqttOptions::new(m.client_id, m.host, m.port.get());
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(false);
    if let (Some(user), Some(password)) = (m.user, m.password) {
        opts.set_credentials(user, password);
    }
    let topic = m.topic;
    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    // Failed flushes keep their rows and are retried on the next tick
    let mut pending: Vec<Row> = Vec::new();
    // Notify once per failure streak, and once when it recovers
    let mut mqtt_down = false;
    let mut flush_failing = false;
    let mut ticker = tokio::time::interval(Duration::from_secs(cfg.flush_secs.get()));
    ticker.tick().await;
    loop {
        tokio::select! {
            ev = eventloop.poll() => match ev {
                Ok(Event::Incoming(Packet::Publish(p))) => match parse(&p.topic, &p.payload) {
                    Ok(row) => pending.push(row),
                    Err(e) => eprintln!("skipping message: {e}"),
                },
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    if mqtt_down {
                        mqtt_down = false;
                        notifier.send("Fishtank monitor", "MQTT connection restored", 3).await;
                    }
                    // (re)subscribe after every connect
                    if let Err(e) = client.subscribe(&topic, QoS::AtLeastOnce).await {
                        eprintln!("subscribe failed: {e}");
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("mqtt error: {e}");
                    if !mqtt_down {
                        mqtt_down = true;
                        notifier.send("Fishtank monitor: MQTT error", &e.to_string(), 7).await;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            },
            _ = ticker.tick() => {
                if pending.is_empty() {
                    continue;
                }
                match flush(&db_url, pg_tls, &pending).await {
                    Ok(n) => {
                        println!("inserted {n} rows");
                        pending.clear();
                        if flush_failing {
                            flush_failing = false;
                            notifier.send("Fishtank monitor", "Database writes restored", 3).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("flush failed, will retry: {e}");
                        if !flush_failing {
                            flush_failing = true;
                            let cause = std::error::Error::source(&*e).map(|c| format!(": {c}")).unwrap_or_default();
                            notifier
                                .send("Fishtank monitor: database error", &format!("{e}{cause}"), 8)
                                .await;
                        }
                    }
                }
            }
        }
    }
}

async fn flush(db_url: &str, pg_tls: bool, rows: &[Row]) -> Result<usize, Box<dyn std::error::Error>> {
    // Build and transform the DataFrame with polars
    let mut df = df!(
        "ts" => rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
        "sensor" => rows.iter().map(|r| r.sensor.as_str()).collect::<Vec<_>>(),
        "value" => rows.iter().map(|r| r.value).collect::<Vec<_>>(),
    )?
    .lazy()
    .filter(col("value").is_finite())
    .select([
        // polars datetimes start at milliseconds, so scale the seconds up
        (col("ts") * lit(1000_i64))
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .alias("time"),
        col("sensor"),
        col("value"),
    ])
    .collect()?;
    let n = df.height();
    if n == 0 {
        return Ok(0);
    }

    // Serialize to CSV in memory and stream into Postgres via COPY
    let mut buf = Vec::new();
    CsvWriter::new(&mut buf)
        .include_header(false)
        .finish(&mut df)?;

    let pg = connect(db_url, pg_tls).await?;
    // Stage via a temp table so redelivered MQTT messages (QoS 1) can't create duplicates
    pg.batch_execute(
        "SET TIME ZONE 'UTC';
         CREATE TABLE IF NOT EXISTS office_fishtank_readings (
            time timestamptz NOT NULL,
            sensor text NOT NULL,
            value double precision NOT NULL);
         CREATE UNIQUE INDEX IF NOT EXISTS office_fishtank_readings_sensor_time
            ON office_fishtank_readings (sensor, time);
         CREATE TEMP TABLE staging (LIKE office_fishtank_readings)",
    )
    .await?;
    let sink = pg
        .copy_in::<_, Bytes>("COPY staging (time, sensor, value) FROM STDIN WITH (FORMAT csv)")
        .await?;
    futures_util::pin_mut!(sink);
    sink.send(Bytes::from(buf)).await?;
    sink.close().await?;
    let inserted = pg
        .execute(
            "INSERT INTO office_fishtank_readings SELECT * FROM staging ON CONFLICT DO NOTHING",
            &[],
        )
        .await?;
    Ok(inserted as usize)
}

async fn connect(url: &str, tls: bool) -> Result<Client, Box<dyn std::error::Error>> {
    let cfg: tokio_postgres::Config = url.parse()?;
    // The connection object does the socket I/O; it runs as a task until the client drops
    if !tls {
        let (client, conn) = cfg.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(conn);
        Ok(client)
    } else {
        let tls = MakeTlsConnector::new(TlsConnector::new()?);
        let (client, conn) = cfg.connect(tls).await?;
        tokio::spawn(conn);
        Ok(client)
    }
}
