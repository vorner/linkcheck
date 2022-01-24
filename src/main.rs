use std::cmp;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::env;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{self, File, OpenOptions};
use std::io::{Error as IoError, Write as _};
use std::iter;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Error};
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Local};
use err_derive::Error;
use futures::future;
use futures::stream;
use futures::StreamExt;
use im::Vector;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use reqwest::multipart::{Form, Part};
use reqwest::{Body, Client, Error as ReqwestError, Method, RequestBuilder};
use serde::de::{DeserializeOwned, Deserializer};
use serde::Deserialize;
use serde_json::{Error as JsonError, Value};
use structopt::StructOpt;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::watch::{channel, Receiver};
use tokio::time::{self, Instant};

const TEMP_DIFF: f32 = 3.0;
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(3600);
const UPLOAD_CHUNK: usize = 1024;
static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .connection_verbose(true)
        .pool_max_idle_per_host(2)
        .http1_only()
        .build()
        .unwrap()
});

struct FormattedDur(Duration);

impl Display for FormattedDur {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let dur = ChronoDuration::from_std(self.0).unwrap_or_else(|_| ChronoDuration::zero());
        write!(f, "{:02}:{:02}", dur.num_hours(), dur.num_minutes() % 60)
    }
}

fn bars(c: char, bars: usize) -> String {
    iter::repeat(c).take(bars).collect()
}

#[derive(Clone)]
enum ProgressStep {
    Start { total: usize },
    Step { current: usize },
    Finished,
    Failed(String),
    Abandoned,
}

struct ProgressReportItem {
    progress: ProgressStep,
    upload: TransferName,
}

struct Reporter {
    name: TransferName,
    sender: UnboundedSender<ProgressReportItem>,
}

impl Reporter {
    fn report(&self, step: ProgressStep) {
        let _ = self.sender.send(ProgressReportItem {
            progress: step,
            upload: self.name.clone(),
        });
    }
}

struct Uploader<'a> {
    sender: UnboundedSender<ProgressReportItem>,
    printer: &'a Printer,
    printer_name: Arc<str>,
    name: Option<Arc<str>>,
    // Names of yet unstarted files
    scheduled: &'a [LoadedFile],
}

impl Uploader<'_> {
    fn report(&self, step: ProgressStep) {
        let name = Arc::clone(self.name.as_ref().expect("Nothing to report on"));
        let _ = self.sender.send(ProgressReportItem {
            progress: step,
            upload: TransferName {
                printer: Arc::clone(&self.printer_name),
                file: name,
            },
        });
    }
    fn report_all(mut self, step: ProgressStep) {
        for scheduled in self.scheduled {
            let _ = self.sender.send(ProgressReportItem {
                progress: step.clone(),
                upload: TransferName {
                    printer: Arc::clone(&self.printer_name),
                    file: Arc::clone(&scheduled.name),
                },
            });
        }
        self.scheduled = &[];
    }
    fn reporter(&self) -> Reporter {
        Reporter {
            sender: self.sender.clone(),
            name: TransferName {
                printer: Arc::clone(&self.printer_name),
                file: Arc::clone(self.name.as_ref().expect("Nothing to report on")),
            },
        }
    }
    async fn upload_one(&mut self, addr: SocketAddr, file: &LoadedFile) -> Result<(), Error> {
        let response = self
            .printer
            .req_any(addr, "api/files/local", Method::POST)
            .multipart(file.form(self.reporter()))
            .timeout(TRANSFER_TIMEOUT)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;

        let resp_str = String::from_utf8_lossy(&response);
        debug!("Received answer {} from {}", resp_str, self.printer_name);

        serde_json::from_slice::<Value>(&response).with_context(|| {
            format!(
                "Malformed response from {}: {}",
                self.printer_name, resp_str
            )
        })?;
        Ok(())
    }
    async fn upload(mut self, addr: SocketAddr) -> Result<(), Error> {
        while !self.scheduled.is_empty() {
            let file = &self.scheduled[0];
            let name = &file.name;
            self.name = Some(Arc::clone(name));
            self.scheduled = &self.scheduled[1..];
            info!("Uploading {} to {}", name, self.printer_name);

            if let Err(e) = self.upload_one(addr, file).await {
                self.report(ProgressStep::Failed(e.to_string()));
                return Err(e);
            };

            info!("Uploaded {} to {}", name, self.printer_name);
            self.report(ProgressStep::Finished);
            self.name.take();
        }

        Ok(())
    }
}

impl Drop for Uploader<'_> {
    fn drop(&mut self) {
        if self.name.is_some() {
            self.report(ProgressStep::Abandoned);
        }
        for scheduled in self.scheduled {
            let _ = self.sender.send(ProgressReportItem {
                progress: ProgressStep::Abandoned,
                upload: TransferName {
                    printer: Arc::clone(&self.printer_name),
                    file: Arc::clone(&scheduled.name),
                },
            });
        }
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct TransferName {
    printer: Arc<str>,
    file: Arc<str>,
}

impl Display for TransferName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}: {}", self.printer, self.file)
    }
}

struct FailedUpload {
    upload: TransferName,
    error: Option<String>,
}

impl Display for FailedUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if let Some(error) = &self.error {
            write!(f, "{} ({})", self.upload, error)
        } else {
            self.upload.fmt(f)
        }
    }
}

struct UploadProgress {
    total: usize,
    done: usize,
    start: Instant,
}

#[derive(Default)]
struct ProgressReportWatch {
    running: BTreeMap<TransferName, UploadProgress>,
    ok: Vec<TransferName>,
    failed: Vec<FailedUpload>,
    width: usize,
}

impl ProgressReportWatch {
    fn new(width: usize) -> Self {
        Self {
            width,
            ..Self::default()
        }
    }
    fn feed(&mut self, report: ProgressReportItem) {
        use ProgressStep::*;

        match report.progress {
            Start { total } => {
                let running = UploadProgress {
                    total,
                    done: 0,
                    start: Instant::now(),
                };
                assert!(self.running.insert(report.upload, running).is_none());
            }
            Step { current } => {
                self.running
                    .get_mut(&report.upload)
                    .expect("Step on non-existent upload")
                    .done = current
            }
            Finished => {
                assert!(self.running.remove(&report.upload).is_some());
                self.ok.push(report.upload);
            }
            Failed(error) => {
                // It's possible to fail by not starting at all
                self.running.remove(&report.upload);
                self.failed.push(FailedUpload {
                    upload: report.upload,
                    error: Some(error),
                });
            }
            Abandoned => {
                self.failed.push(FailedUpload {
                    upload: report.upload,
                    error: None,
                });
            }
        }
    }
}

fn speed(start: Instant, transfered: usize) -> u32 {
    let mut elapsed = start.elapsed();
    if elapsed == Duration::default() {
        elapsed = Duration::from_millis(1);
    }
    let speed = (transfered as f64) / elapsed.as_secs_f64();
    speed.round() as _
}

impl Display for ProgressReportWatch {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let bar = bars('=', self.width);
        writeln!(f, "{}", bar)?;
        for (upload, progress) in &self.running {
            writeln!(
                f,
                "{}: {}/{} @{}",
                upload,
                progress.done,
                progress.total,
                speed(progress.start, progress.done),
            )?;
            let squares = self.width * progress.done / progress.total;
            writeln!(f, "{}", bars('#', squares))?;
        }

        writeln!(f, "{}", bar)?;
        writeln!(f, "Uploaded:")?;
        for name in &self.ok {
            writeln!(f, "• {}", name)?;
        }

        writeln!(f, "{}", bar)?;
        writeln!(f, "Failed:")?;
        for name in &self.failed {
            writeln!(f, "• {}", name)?;
        }
        write!(f, "{}", bar)?;

        Ok(())
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
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JobInner {
    file: JobFile,
}

#[derive(Debug, Deserialize)]
struct JobResponse {
    progress: Option<Progress>,
    job: Option<JobInner>,
}

#[derive(Clone)]
struct LoadedFile {
    /// The name to pass to the other side (not necessarily the same as the file name)
    name: Arc<str>,
    /// The data to pass there
    data: Bytes,
    /// Start print?
    start: bool,
}

impl LoadedFile {
    fn load(f: &Path, original_name: bool, start: bool) -> Result<Self, Error> {
        let data = fs::read(f)
            .with_context(|| format!("Couldn't load {}", f.display()))?
            .into();

        match f.extension() {
            Some(e) if e.eq_ignore_ascii_case("gco") || e.eq_ignore_ascii_case("gcode") => (),
            _ => bail!("{} isn't gcode", f.display()),
        };

        let fname = f
            .file_name()
            .ok_or_else(|| anyhow!("No file name in {}", f.display()))?
            .to_string_lossy();

        let name = if original_name {
            fname.into()
        } else {
            // Note: Seems like fat filesystems don't like ':' in the name!
            format!("{}-{}", Local::now().format("%F-%H-%M-%S"), fname).into()
        };

        Ok(Self { data, name, start })
    }
    fn form(&self, reporter: Reporter) -> Form {
        let chunk_cnt = (self.data.len() + UPLOAD_CHUNK - 1) / UPLOAD_CHUNK;
        reporter.report(ProgressStep::Start {
            total: self.data.len(),
        });
        let data = self.data.clone();
        let name = Arc::clone(&self.name);
        let watched_stream = (0..chunk_cnt).map(move |i| {
            let start = i * UPLOAD_CHUNK;
            reporter.report(ProgressStep::Step { current: start });
            let end = cmp::min(start + UPLOAD_CHUNK, data.len());
            debug!("Preparing chunk {}/{} of {}", i, chunk_cnt, name);
            Ok::<_, Infallible>(data.slice(start..end))
        });
        let watched_body = Body::wrap_stream(stream::iter(watched_stream));
        let payload = Part::stream_with_length(watched_body, self.data.len() as u64)
            .file_name(String::from(&*self.name));
        Form::new()
            .text("print", if self.start { "true" } else { "false" })
            .part("file", payload)
    }
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Upload a file to the printer.
    Upload {
        /// The file(s) to upload.
        #[structopt(parse(from_os_str))]
        file: Vec<PathBuf>,

        /// Keep the original name.
        ///
        /// Usually, the tool renames the uploaded file to include timestamp, both for sorting and
        /// to avoid duplicates. This avoids it and leaves the name intact.
        #[structopt(long, short)]
        original_name: bool,

        /// Ask the printer to start the print right away.
        ///
        /// In case multiple files are uploaded, this is applied to the first one (we assume we are
        /// uploading faster than printing).
        #[structopt(long, short)]
        start: bool,
    },
    /// Grab the currently printed file.
    Grab,
}

/// Spy & do statistics on a PrusaLink printer.
#[derive(Debug, StructOpt)]
struct Opts {
    /// What printers to query.
    ///
    /// If not specified, all printers from the config file are used.
    #[structopt(short, long, number_of_values(1))]
    printer: Vec<String>,

    /// Number of display columns to use; sets the history size.
    #[structopt(short, long)]
    width: Option<usize>,

    /// Interval between iterations, in milliseconds.
    #[structopt(short, long, default_value = "1000")]
    interval: u64,

    /// Maximum number of failures before considering the printer down.
    #[structopt(short, long, default_value = "5")]
    max_fail: usize,

    /// Configuration file.
    ///
    /// Contains connection info for the printers. Yaml.
    #[structopt(short, long, parse(from_os_str))]
    config: Option<PathBuf>,

    /// Use raw ugly output to preserve logs and everything.
    #[structopt(long, short)]
    ugly: bool,

    /// A command to execute on selected printer(s) instead of watching them.
    #[structopt(subcommand)]
    cmd: Option<Command>,
}

#[derive(Copy, Clone, Debug)]
struct WatchOpts {
    width: usize,
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
    Inconsistent,
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
    fn from_parts(printer: PrinterResponse, job: JobResponse) -> Result<Self, StepErr> {
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
        let result = if active {
            let progress = job.progress.ok_or(StepErr::Inconsistent)?;
            let job = job.job.ok_or(StepErr::Inconsistent)?;
            Snapshot::Active {
                done: progress.completion * 100.0,
                time_spent: Duration::from_secs(progress.print_time),
                time_left: Duration::from_secs(progress.print_time_left),
                telemetry,
                paused,
                name: job.file.name,
            }
        } else {
            Snapshot::Ready { telemetry }
        };
        Ok(result)
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
            Inconsistent => 'I',
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
    name: Arc<str>,
    printer: Printer,
    opts: WatchOpts,
}

impl PrinterInfo {
    fn watch(self) -> Receiver<PrinterStatus> {
        let mut status = PrinterStatus {
            info: Arc::new(self),
            selected: None,
            failed: 0,
            history: Vector::new(),
            alive: false,
            last_duration: Duration::default(),
            success_reqs: 0,
            failed_reqs: 0,
        };
        let (sender, receiver) = channel(status.clone());
        tokio::spawn(async move {
            loop {
                let start = Instant::now();
                let end = start + status.info.opts.interval;

                status.step().await;

                let _ = sender.send(status.clone());

                time::sleep_until(end).await;
            }
        });
        receiver
    }

    async fn select(&self) -> Result<SocketAddr, StepErr> {
        if self.printer.addr.is_empty() {
            return Err(StepErr::NoIps);
        } else {
            let candidates = self.printer.addr.iter().map(|&addr| {
                Box::pin(async move {
                    let version: VersionResponse = self.printer.req(addr, "api/version").await?;
                    debug!(
                        "Received version {}'s {:?} from {}",
                        self.name, version, addr
                    );
                    Ok(addr)
                })
            });
            future::select_ok(candidates).await.map(|(addr, _)| addr)
        }
    }

    async fn upload(
        &self,
        files: Arc<[LoadedFile]>,
        progress: UnboundedSender<ProgressReportItem>,
    ) -> Result<(), Error> {
        let uploader = Uploader {
            name: None,
            printer: &self.printer,
            printer_name: Arc::clone(&self.name),
            sender: progress,
            scheduled: &files,
        };

        debug!("Selection of {} started", self.name);

        let addr = match self.select().await {
            Ok(addr) => addr,
            Err(e) => {
                uploader.report_all(ProgressStep::Failed(e.to_string()));
                return Err(e.into());
            }
        };

        info!("Going to use {} for {}", addr, self.name);

        uploader.upload(addr).await?;

        info!("Everything uploaded to {}", self.name);

        Ok(())
    }

    async fn grab(&self, progress: UnboundedSender<DownloadProgressItem>) {
        macro_rules! rtry {
            ($action: expr, $err: expr) => {
                match $action {
                    Ok(value) => value,
                    Err(err) => {
                        let _ = progress.send($err(err));
                        return;
                    }
                }
            };
        }

        let addr = rtry!(self.select().await, |err: StepErr| {
            DownloadProgressItem::PrinterError {
                printer: Arc::clone(&self.name),
                err: err.to_string(),
            }
        });

        let job: JobResponse = rtry!(
            self.printer.req::<JobResponse>(addr, "api/job").await,
            |err: StepErr| {
                DownloadProgressItem::PrinterError {
                    printer: Arc::clone(&self.name),
                    err: err.to_string(),
                }
            }
        );

        let job = match job.job {
            None => {
                let _ = progress.send(DownloadProgressItem::NotPrinting(Arc::clone(&self.name)));
                return;
            }
            Some(job) => job,
        };

        let file: Arc<str> = job.file.name.into();
        let path = job.file.path.as_deref().unwrap_or(&file);
        let name = TransferName {
            printer: Arc::clone(&self.name),
            file: Arc::clone(&file),
        };

        let response = rtry!(
            self.printer
                .req_any(addr, &format!("api/files/{}", path), Method::GET)
                .timeout(TRANSFER_TIMEOUT)
                .send()
                .await
                .and_then(|r| r.error_for_status()),
            |err: ReqwestError| {
                DownloadProgressItem::TransferError {
                    download: name,
                    err: err.to_string(),
                }
            }
        );

        let mut data = response.bytes_stream();
        let mut done = 0;
        let mut dest = rtry!(
            OpenOptions::new().create_new(true).write(true).open(&*file),
            |err: IoError| {
                DownloadProgressItem::TransferError {
                    download: name,
                    err: err.to_string(),
                }
            }
        );

        while let Some(chunk) = data.next().await {
            let chunk = rtry!(chunk, |err: ReqwestError| {
                DownloadProgressItem::TransferError {
                    download: name,
                    err: err.to_string(),
                }
            });
            done += chunk.len();
            rtry!(dest.write_all(&chunk), |err: IoError| {
                DownloadProgressItem::TransferError {
                    download: name,
                    err: err.to_string(),
                }
            });

            let _ = progress.send(DownloadProgressItem::Step {
                download: name.clone(),
                done,
            });
        }

        let _ = progress.send(DownloadProgressItem::Done(name));
    }
}

enum DownloadProgressItem {
    Step { download: TransferName, done: usize },
    Done(TransferName),
    NotPrinting(Arc<str>),
    PrinterError { printer: Arc<str>, err: String },
    TransferError { download: TransferName, err: String },
}

#[derive(Debug, Error)]
enum StepErr {
    #[error(display = "HTTP error: {}", _0)]
    Reqwest(#[error(source)] ReqwestError),
    #[error(display = "No IPs to connect to")]
    NoIps,
    #[error(display = "Malformed error: {}", err)]
    Malformed {
        #[error(source)]
        err: JsonError,
        body: Option<Bytes>,
    },
    #[error(display = "Inconsistent data")]
    Inconsistent,
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
            StepErr::Inconsistent => Snapshot::Inconsistent,
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
    success_reqs: u32,
    failed_reqs: u32,
}

impl PrinterStatus {
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
            let selected = self.info.select().await?;
            info!("Selected {} for {}", selected, self.info.name);
            self.selected = Some(selected);
        }

        let (printer, job) = self.get_info().await?;

        Ok(Snapshot::from_parts(printer, job)?)
    }

    fn store_snapshot(&mut self, snapshot: Snapshot) {
        self.history.push_back(snapshot);
        while self.history.len() > self.info.opts.width {
            self.history.pop_front();
        }
    }

    async fn step(&mut self) {
        let now = Instant::now();
        match self.step_inner().await {
            Ok(snapshot) => {
                self.failed = 0;
                self.alive = true;
                self.success_reqs = self.success_reqs.saturating_add(1);
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
                        self.success_reqs = 0;
                        self.failed_reqs = 0;
                    }
                } else {
                    self.failed_reqs = self.failed_reqs.saturating_add(1);
                }
                self.store_snapshot(e.into());
            }
        }
        let took = now.elapsed();

        debug!("Step on {} took {:?}", self.info.name, took);
        self.last_duration = took;
    }
}

impl Display for PrinterStatus {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        writeln!(f, "{}", bars('=', self.info.opts.width))?;
        if let Some(addr) = self.selected {
            writeln!(
                f,
                "{}: {} [{:?}] ✔: {:?} ✘: {:?}",
                self.info.name, addr, self.last_duration, self.success_reqs, self.failed_reqs
            )?;
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
                    let squares = (done / 100.0 * self.info.opts.width as f32) as usize;
                    writeln!(f, "{}", bars('#', squares))?;
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
    fn req_any(&self, addr: SocketAddr, path: &str, method: Method) -> RequestBuilder {
        let url = format!("http://{}/{}", addr, path);
        HTTP_CLIENT
            .request(method, url)
            .header("X-Api-Key", &self.key)
    }
    async fn req<R: DeserializeOwned>(&self, addr: SocketAddr, path: &str) -> Result<R, StepErr> {
        let response = self
            .req_any(addr, path, Method::GET)
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
}

#[derive(Debug, Default, Deserialize)]
struct DisplayConfig {
    width: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct Config {
    printers: BTreeMap<String, Printer>,

    #[serde(default)]
    display: DisplayConfig,
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

    let display_width = opts.width.or(config.display.width).unwrap_or(180);

    let wopts = WatchOpts {
        width: display_width,
        max_fail: opts.max_fail,
        interval: Duration::from_millis(opts.interval),
    };
    let printers = printers.into_iter().map(|(name, printer)| PrinterInfo {
        printer,
        name: name.into(),
        opts: wopts,
    });

    match opts.cmd {
        Some(Command::Upload {
            file,
            original_name,
            start,
        }) => {
            let files = file
                .into_iter()
                .map(|file| {
                    let f = LoadedFile::load(&file, original_name, start);
                    if let Ok(f) = &f {
                        debug!("Loaded {} ({} bytes)", file.display(), f.data.len());
                    }
                    f
                })
                .collect::<Result<_, Error>>()?;

            debug!("Data to upload loaded");

            let (sender, mut receiver) = mpsc::unbounded_channel();
            let uploads = printers.into_iter().map(|printer| {
                let files = Arc::clone(&files);
                let sender = sender.clone();

                async move {
                    let res = printer.upload(files, sender).await;
                    if let Err(e) = &res {
                        error!("Upload to {} failed: {:?}", printer.name, e);
                    }

                    res
                }
            });

            let uploads = future::join_all(uploads);
            drop(sender);

            let watch = async {
                let mut progress = ProgressReportWatch::new(display_width);
                while let Some(report) = receiver.recv().await {
                    progress.feed(report);

                    // Try to process as many things as available before redrawing.
                    while let Ok(report) = receiver.try_recv() {
                        progress.feed(report);
                    }

                    if !opts.ugly {
                        print!("\x1B[2J\x1B[1;1H");
                    }

                    println!("{}", progress);
                }

                progress
            };

            let progress = future::join(uploads, watch).await.1;

            let failed = &progress.failed;

            if failed.len() > 0 {
                eprintln!("Failures:");
                for f in failed {
                    eprintln!("• {}", f);
                }
                bail!("Failed to upload to {} printers", failed.len());
            }
        }
        Some(Command::Grab) => {
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let downloads = printers.into_iter().map(|printer| {
                let sender = sender.clone();

                async move {
                    printer.grab(sender).await;
                }
            });
            let downloads = future::join_all(downloads);
            drop(sender);

            let mut errors = Vec::new();
            let mut running = BTreeMap::new();

            let watch = async {
                while let Some(report) = receiver.recv().await {
                    match report {
                        DownloadProgressItem::Step { download, done } => {
                            running
                                .entry(download)
                                .or_insert_with(|| (Instant::now(), 0))
                                .1 = done;
                        }
                        DownloadProgressItem::Done(name) => {
                            running.remove(&name);
                        }
                        DownloadProgressItem::NotPrinting(_) => (),
                        DownloadProgressItem::PrinterError { printer, err } => {
                            errors.push(format!("{}: {}", printer, err));
                        }
                        DownloadProgressItem::TransferError { download, err } => {
                            running.remove(&download);
                            errors.push(format!("{}: {}", download, err));
                        }
                    }

                    if !opts.ugly {
                        print!("\x1B[2J\x1B[1;1H");
                    }

                    for (transfer, status) in &running {
                        println!("{}: {} @{}", transfer, status.1, speed(status.0, status.1));
                    }

                    println!("{}", bars('=', display_width));

                    for err in &errors {
                        println!("{}", err);
                    }
                }
            };

            future::join(downloads, watch).await;

            for err in errors {
                eprintln!("{}\n", err);
            }
        }
        None => {
            let mut watched_printers = printers
                .into_iter()
                .map(PrinterInfo::watch)
                .collect::<Vec<_>>();

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
    }

    Ok(())
}
