use std::collections::BTreeMap;
use std::env;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::File;
use std::iter;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Error};
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Local};
use err_derive::Error;
use futures::future;
use im::Vector;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use reqwest::{Client, Error as ReqwestError};
use serde::de::{DeserializeOwned, Deserializer};
use serde::Deserialize;
use serde_json::Error as JsonError;
use structopt::StructOpt;
use tokio::sync::watch::{channel, Receiver};
use tokio::time::{self, Instant};

const TEMP_DIFF: f32 = 3.0;
static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .http1_title_case_headers() // "Bug"/not-implemented proper http
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .connection_verbose(true)
        .pool_max_idle_per_host(1)
        .http1_only()
        .build()
        .unwrap()
});

struct FormattedDur(Duration);

impl Display for FormattedDur {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let dur = ChronoDuration::from_std(self.0).unwrap_or_else(|_| ChronoDuration::zero());
        write!(f, "{:02}:{:02}:{:02}", dur.num_hours(), dur.num_minutes() % 60, dur.num_seconds() % 60)
    }
}

#[allow(dead_code)] // Used as part of debug impl
#[derive(Debug, Deserialize)]
struct VersionResponse {
    server: String,
    text: String,
    hostname: String,
}

fn de_bool_or_int<'de, D: Deserializer<'de>>(de: D) -> Result<bool, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrInt {
        Bool(bool),
        Int(i64),
    }

    let val = BoolOrInt::deserialize(de)?;
    Ok(match val {
        BoolOrInt::Bool(b) => b,
        BoolOrInt::Int(i) => i != 0,
    })
}

#[derive(Debug, Deserialize)]
struct Flags {
    #[serde(deserialize_with = "de_bool_or_int")]
    printing: bool,
    #[serde(deserialize_with = "de_bool_or_int")]
    paused: bool,
    #[serde(deserialize_with = "de_bool_or_int")]
    pausing: bool,
}

#[derive(Debug, Deserialize)]
struct PrinterState {
    flags: Flags,
}

#[derive(Debug, Deserialize)]
struct OneTemp {
    target: f32,
    actual: f32,
}

#[derive(Debug, Deserialize)]
struct Temperature {
    tool0: OneTemp,
    bed: OneTemp,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PrinterTelemetry {
    z_height: f32,
    material: String,
}

#[derive(Debug, Deserialize)]
struct PrinterResponse {
    state: PrinterState,
    temperature: Temperature,
    telemetry: PrinterTelemetry,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Progress {
    completion: f32,
    print_time: u64,
    print_time_left: u64,
}

#[derive(Debug, Deserialize)]
struct JobFile {
    name: String,
}

#[derive(Debug, Deserialize)]
struct JobInner {
    file: JobFile,
}

#[derive(Debug, Deserialize)]
struct JobResponse {
    progress: Progress,
    job: JobInner,
}

/// Spy & do statistics on a PrusaLink printer.
#[derive(Debug, StructOpt)]
struct Opts {
    /// What printers to query.
    ///
    /// If not specified, all printers from the config file are used.
    #[structopt(short, long)]
    printer: Vec<String>,

    /// Number of history samples.
    #[structopt(short, long, default_value = "120")]
    history: usize,

    /// Interval between iterations, in milliseconds.
    #[structopt(short, long, default_value = "1000")]
    interval: u64,

    /// Maximum number of failures before considering the printer down.
    #[structopt(short, long, default_value = "5")]
    max_fail: usize,

    /// Configuration file.
    ///
    /// Contains connection info for the printers. Yaml.
    #[structopt(parse(from_os_str))]
    config: Option<PathBuf>,

    /// Use raw ugly output to preserve logs and everything.
    #[structopt(long, short)]
    ugly: bool,
}

#[derive(Copy, Clone, Debug)]
struct WatchOpts {
    history: usize,
    max_fail: usize,
    interval: Duration,
}

#[derive(Clone, Debug)]
struct Telemetry {
    nozzle_current: f32,
    nozzle_target: f32,
    bed_current: f32,
    bed_target: f32,
    z: f32,
    material: String,
}

impl Display for Telemetry {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "N: {:3.1}/{:3.1} B: {:3.1}/{:3.1} Z: {:2.1} [{:^5}]",
            self.nozzle_current,
            self.nozzle_target,
            self.bed_current,
            self.bed_target,
            self.z,
            self.material
        )
    }
}

#[derive(Clone, Debug)]
enum Snapshot {
    TimeoutErr,
    ConnectErr,
    HttpErr,
    ConfigErr,
    MalformedErr,
    Ready {
        telemetry: Telemetry,
    },
    Active {
        done: f32,
        time_spent: Duration,
        time_left: Duration,
        telemetry: Telemetry,
        paused: bool,
        name: String,
    },
}

impl Snapshot {
    fn from_parts(printer: PrinterResponse, job: JobResponse) -> Self {
        let telemetry = Telemetry {
            bed_current: printer.temperature.bed.actual,
            bed_target: printer.temperature.bed.target,
            nozzle_current: printer.temperature.tool0.actual,
            nozzle_target: printer.temperature.tool0.target,
            z: printer.telemetry.z_height,
            material: printer.telemetry.material,
        };
        let active = printer.state.flags.printing
            || printer.state.flags.paused
            || printer.state.flags.pausing;
        let paused = printer.state.flags.paused || printer.state.flags.pausing;
        if active {
            Snapshot::Active {
                done: job.progress.completion * 100.0,
                time_spent: Duration::from_secs(job.progress.print_time),
                time_left: Duration::from_secs(job.progress.print_time_left),
                telemetry,
                paused,
                name: job.job.file.name,
            }
        } else {
            Snapshot::Ready { telemetry }
        }
    }

    fn sig(&self) -> char {
        use Snapshot::*;
        match self {
            TimeoutErr => 'T',
            ConnectErr => 'C',
            HttpErr => 'H',
            ConfigErr => '?',
            MalformedErr => 'M',
            Ready { .. } => '_',
            Active { paused: false, .. } => '#',
            Active { paused: true, .. } => '*',
        }
    }

    fn temp_sig<F: FnOnce(&Telemetry) -> (f32, bool)>(&self, f: F) -> char {
        match self {
            Snapshot::Active { telemetry, .. } | Snapshot::Ready { telemetry } => {
                let (diff, on) = f(telemetry);
                if !on {
                    '.'
                } else if diff < -TEMP_DIFF {
                    '↑'
                } else if diff > TEMP_DIFF {
                    '↓'
                } else {
                    '-'
                }
            }
            _ => ' ',
        }
    }

    fn temp_sig_nozzle(&self) -> char {
        self.temp_sig(|tele| {
            (
                tele.nozzle_current - tele.nozzle_target,
                tele.nozzle_target > 0.0,
            )
        })
    }

    fn temp_sig_bed(&self) -> char {
        self.temp_sig(|tele| (tele.bed_current - tele.bed_target, tele.bed_target > 0.0))
    }
}

struct PrinterInfo {
    name: String,
    printer: Printer,
    opts: WatchOpts,
}

#[derive(Debug, Error)]
enum StepErr {
    #[error(display = "HTTP error")]
    Reqwest(#[error(source)] ReqwestError),
    #[error(display = "No IPs to connect to")]
    NoIps,
    #[error(display = "Malformed error")]
    Malformed {
        #[error(source)]
        err: JsonError,
        body: Option<Bytes>,
    },
}

impl From<StepErr> for Snapshot {
    fn from(e: StepErr) -> Self {
        match e {
            StepErr::NoIps => Snapshot::ConfigErr,
            StepErr::Reqwest(re) if re.is_timeout() => Snapshot::TimeoutErr,
            StepErr::Reqwest(re) if re.is_connect() => Snapshot::ConnectErr,
            StepErr::Reqwest(re) if re.is_decode() => Snapshot::MalformedErr,
            StepErr::Malformed { .. } => Snapshot::MalformedErr,
            StepErr::Reqwest(_) => Snapshot::HttpErr,
        }
    }
}

#[derive(Clone)]
struct PrinterStatus {
    info: Arc<PrinterInfo>,
    selected: Option<SocketAddr>,
    failed: usize,
    history: Vector<Snapshot>,
    alive: bool,
    last_duration: Duration,
}

impl PrinterStatus {
    async fn select(&self) -> Result<SocketAddr, StepErr> {
        if self.info.printer.addr.is_empty() {
            return Err(StepErr::NoIps);
        } else {
            let candidates = self.info.printer.addr.iter().map(|&addr| {
                Box::pin(async move {
                    let version: VersionResponse =
                        self.info.printer.req(addr, "api/version").await?;
                    debug!(
                        "Received version {}'s {:?} from {}",
                        self.info.name, version, addr
                    );
                    Ok(addr)
                })
            });
            future::select_ok(candidates).await.map(|(addr, _)| addr)
        }
    }

    async fn req<R: DeserializeOwned>(&self, path: &str) -> Result<R, StepErr> {
        self.info
            .printer
            .req(self.selected.expect("No address selected"), path)
            .await
    }

    async fn get_info(&self) -> Result<(PrinterResponse, JobResponse), StepErr> {
        future::try_join(self.req("api/printer"), self.req("api/job")).await
    }

    async fn step_inner(&mut self) -> Result<Snapshot, StepErr> {
        if self.selected.is_none() {
            let selected = self.select().await?;
            info!("Selected {} for {}", selected, self.info.name);
            self.selected = Some(selected);
        }

        let (printer, job) = self.get_info().await?;

        Ok(Snapshot::from_parts(printer, job))
    }

    fn store_snapshot(&mut self, snapshot: Snapshot) {
        self.history.push_back(snapshot);
        while self.history.len() > self.info.opts.history {
            self.history.pop_front();
        }
    }

    async fn step(&mut self) {
        let now = Instant::now();
        match self.step_inner().await {
            Ok(snapshot) => {
                self.failed = 0;
                self.alive = true;
                self.store_snapshot(snapshot);
            }
            Err(e) => {
                warn!("Snapshot on {} failed: {:?}", self.info.name, e);
                self.failed = self.failed.saturating_add(1);
                if self.failed >= self.info.opts.max_fail {
                    let previous = self.selected.take();
                    if self.alive {
                        warn!(
                            "Printer {} is no longer available at {}",
                            self.info.name,
                            previous.unwrap()
                        );
                        self.alive = false;
                    }
                }
                self.store_snapshot(e.into());
            }
        }
        let took = now.elapsed();

        debug!(
            "Step on {} took {:?}",
            self.info.name,
            took
        );
        self.last_duration = took;
    }
}

impl Display for PrinterStatus {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        writeln!(f, "{}", iter::repeat('=').take(self.info.opts.history).collect::<String>())?;
        if let Some(addr) = self.selected {
            writeln!(f, "{}: {} [{:?}]", self.info.name, addr, self.last_duration)?;
        } else {
            writeln!(
                f,
                "{}: <Not connected> [{:?}]",
                self.info.name, self.last_duration
            )?;
        }

        for snap in &self.history {
            write!(f, "{}", snap.sig())?;
        }
        writeln!(f)?;

        for snap in &self.history {
            write!(f, "{}", snap.temp_sig_nozzle())?;
        }
        writeln!(f)?;

        for snap in &self.history {
            write!(f, "{}", snap.temp_sig_bed())?;
        }

        if let Some(last) = self.history.last() {
            match last {
                Snapshot::Ready { telemetry: temps } => {
                    writeln!(f)?;
                    write!(f, " ---% {}", temps)?;
                }
                Snapshot::Active {
                    telemetry: temps,
                    done,
                    time_left,
                    time_spent,
                    name,
                    paused,
                    ..
                } => {
                    writeln!(f)?;
                    writeln!(f, "{:>3}% {}", done, temps)?;
                    let bars = (done / 100.0 * self.info.opts.history as f32) as usize;
                    writeln!(f, "{}", iter::repeat('#').take(bars).collect::<String>())?;
                    let end: DateTime<Local> = (SystemTime::now() + *time_left).into();
                    write!(
                        f,
                        "Printing: {} Left: {} Total: {} End: {}{} <{}>",
                        FormattedDur(*time_spent),
                        FormattedDur(*time_left),
                        FormattedDur(*time_spent + *time_left),
                        end.format("%F %R"),
                        if *paused { " (paused)" } else { "" },
                        name,
                    )?;
                }
                _ => (),
            }
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct Printer {
    addr: Vec<SocketAddr>,
    key: String,
}

impl Printer {
    async fn req<R: DeserializeOwned>(&self, addr: SocketAddr, path: &str) -> Result<R, StepErr> {
        let url = format!("http://{}/{}", addr, path);
        let response = HTTP_CLIENT
            .get(url)
            .header("X-Api-Key", &self.key)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        match serde_json::from_slice(&response) {
            Ok(resp) => Ok(resp),
            Err(err) => {
                error!(
                    "Malformed response from {} on {}: {}: {}",
                    addr,
                    path,
                    err,
                    String::from_utf8_lossy(&response)
                );
                Err(StepErr::Malformed {
                    err,
                    body: Some(response),
                })
            }
        }
    }
    fn watch(self, name: String, opts: WatchOpts) -> Receiver<PrinterStatus> {
        let mut status = PrinterStatus {
            info: Arc::new(PrinterInfo {
                name,
                printer: self,
                opts,
            }),
            selected: None,
            failed: 0,
            history: Vector::new(),
            alive: false,
            last_duration: Duration::default(),
        };
        let (sender, receiver) = channel(status.clone());
        tokio::spawn(async move {
            loop {
                let start = Instant::now();
                let end = start + opts.interval;

                status.step().await;

                let _ = sender.send(status.clone());

                time::sleep_until(end).await;
            }
        });
        receiver
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    printers: BTreeMap<String, Printer>,
}

impl Config {
    fn get(p: &Path) -> Result<Self, Error> {
        let file = File::open(p)?;
        let cfg = serde_yaml::from_reader(file)?;
        Ok(cfg)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    env_logger::try_init()?;

    let opts = Opts::from_args();
    let config_path = opts.config.unwrap_or_else(|| {
        let mut path = PathBuf::from(env::var_os("HOME").unwrap_or_default());
        path.push(".printers.yaml");
        path
    });
    debug!("Reading config {}", config_path.display());
    let mut config = Config::get(&config_path)
        .with_context(|| format!("Failed to read config file {}", config_path.display()))?;

    let printers = if opts.printer.is_empty() {
        config.printers
    } else {
        let mut pruned = BTreeMap::new();
        for name in opts.printer {
            match config.printers.remove(&name) {
                Some(printer) => {
                    pruned.insert(name, printer);
                }
                None => bail!(
                    "Printer {} not configured in {}",
                    name,
                    config_path.display()
                ),
            }
        }
        pruned
    };

    debug!("Going to watch printers {:?}", printers.keys());

    let mut watched_printers = Vec::new();
    let wopts = WatchOpts {
        history: opts.history,
        max_fail: opts.max_fail,
        interval: Duration::from_millis(opts.interval),
    };
    for (name, printer) in printers {
        let status = printer.watch(name.clone(), wopts);
        watched_printers.push(status);
    }

    if watched_printers.is_empty() {
        bail!("No printers to watch");
    }

    loop {
        if !opts.ugly {
            print!("\x1B[2J\x1B[1;1H");
        }

        for printer in &mut watched_printers {
            println!("{}", *printer.borrow_and_update());
        }

        let wait_for_changes = watched_printers.iter_mut().map(|p| Box::pin(p.changed()));
        future::select_all(wait_for_changes).await.0?;
    }
}
