//! Metrics pushed to an OpenTelemetry collector, when `observability.otlp.endpoint` names one.
//!
//! OTLP over HTTP with protobuf bodies, on reqwest's blocking client: the SDK's periodic reader
//! exports from a thread of its own and needs no async runtime to do it. gRPC would bring tonic,
//! and want a tokio runtime to export from.
//!
//! Every setting the exporter takes comes from the configuration. The exporter reads `OTEL_`
//! variables for any it is not given, and merges `OTEL_EXPORTER_OTLP_HEADERS` into its headers
//! whatever it is given; openqtt-config refuses to start with any `OTEL_` variable set, so none
//! can change it. The headers of `headers_file` ride on the HTTP client instead, where nothing
//! rewrites them. The client speaks TLS with rustls on aws-lc-rs, the binary's one TLS stack and
//! crypto provider, trusting the certificates of `ca_file` or else the Mozilla roots built in,
//! never the system's store or a proxy from `HTTPS_PROXY`, both of which reqwest would otherwise
//! read from the environment.

use std::path::Path;
use std::sync::Arc;

use openqtt_config::{Otlp, SecretFile, Settings};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;

/// The path below the collector's base URL that takes metrics.
const METRICS_PATH: &str = "/v1/metrics";

/// What [`start`] left running, for [`Telemetry::shutdown`] to stop.
pub(crate) struct Telemetry {
    provider: Option<SdkMeterProvider>,
}

/// Starts pushing metrics if an endpoint is configured, and installs the meter provider for the
/// whole process: every instrument made after this records to it.
///
/// It runs before any async runtime starts: reqwest's blocking client cannot be made inside one.
pub(crate) fn start(settings: &Settings) -> Result<Telemetry, String> {
    let provider = meter_provider(settings)?;
    if let Some(provider) = &provider {
        opentelemetry::global::set_meter_provider(provider.clone());
    }
    Ok(Telemetry { provider })
}

impl Telemetry {
    /// Pushes what has not been pushed, and stops.
    pub(crate) fn shutdown(self) {
        if let Some(provider) = self.provider
            && let Err(error) = provider.shutdown()
        {
            tracing::warn!(%error, "the last metrics were not pushed");
        }
    }
}

/// The meter provider the settings call for, or `None` when they name no endpoint.
pub(crate) fn meter_provider(settings: &Settings) -> Result<Option<SdkMeterProvider>, String> {
    let otlp = &settings.observability.otlp;
    let Some(endpoint) = &otlp.endpoint else {
        return Ok(None);
    };
    let exporter = MetricExporter::builder()
        .with_http()
        .with_temporality(Temporality::Cumulative)
        .with_http_client(http_client(otlp)?)
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint.join(METRICS_PATH))
        .with_timeout(otlp.timeout.as_std())
        .build()
        .map_err(|error| format!("observability.otlp: {error}"))?;
    let reader = PeriodicReader::builder(exporter)
        .with_interval(otlp.interval.as_std())
        .build();
    Ok(Some(
        SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource(settings))
            .build(),
    ))
}

/// Who is pushing: the service, its version, its cluster and this node. Built empty rather than
/// from the SDK's detectors, which would read `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`.
fn resource(settings: &Settings) -> Resource {
    let cluster = &settings.cluster;
    let mut attributes = vec![
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("service.namespace", cluster.name.clone()),
    ];
    if let Some(node) = &cluster.node_name {
        attributes.push(KeyValue::new("service.instance.id", node.clone()));
    }
    Resource::builder_empty()
        .with_service_name("openqtt")
        .with_attributes(attributes)
        .build()
}

/// The HTTP client the exporter sends with.
fn http_client(otlp: &Otlp) -> Result<reqwest::blocking::Client, String> {
    let headers = match &otlp.headers_file {
        Some(file) => headers(file)?,
        None => HeaderMap::new(),
    };
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(otlp.timeout.as_std())
        .default_headers(headers)
        .tls_backend_preconfigured(tls(otlp.ca_file.as_deref())?)
        .build()
        .map_err(|error| format!("observability.otlp: cannot make the HTTP client: {error}"))
}

/// The headers of `observability.otlp.headers_file`: one `name: value` per line, blank lines
/// and lines starting `#` skipped. Each value is marked sensitive, so the HTTP stack never logs
/// it, and no error repeats a line, which may hold a token.
fn headers(file: &SecretFile) -> Result<HeaderMap, String> {
    let path = file.path().display();
    let refuse = |why: String| format!("observability.otlp.headers_file: {why}");
    let secret = file
        .read()
        .map_err(|error| refuse(format!("cannot read {path}: {error}")))?;
    let text =
        std::str::from_utf8(secret.expose()).map_err(|_| refuse(format!("{path} is not UTF-8")))?;
    let mut headers = HeaderMap::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let malformed = || refuse(format!("line {} of {path} is not `name: value`", index + 1));
        let (name, value) = line.split_once(':').ok_or_else(malformed)?;
        let name = HeaderName::from_bytes(name.trim().as_bytes()).map_err(|_| malformed())?;
        let mut value = HeaderValue::from_str(value.trim()).map_err(|_| malformed())?;
        if value.is_empty() {
            return Err(malformed());
        }
        value.set_sensitive(true);
        headers.append(name, value);
    }
    Ok(headers)
}

/// TLS for the exporter: rustls on aws-lc-rs, trusting the certificates in `ca_file`, or the
/// Mozilla roots without one.
fn tls(ca_file: Option<&Path>) -> Result<rustls::ClientConfig, String> {
    let roots = match ca_file {
        None => webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        Some(path) => {
            let refuse = |why: String| format!("observability.otlp.ca_file: {why}");
            let mut roots = rustls::RootCertStore::empty();
            let certificates = CertificateDer::pem_file_iter(path)
                .map_err(|error| refuse(format!("cannot read {}: {error}", path.display())))?;
            for certificate in certificates {
                let certificate =
                    certificate.map_err(|error| refuse(format!("{}: {error}", path.display())))?;
                roots
                    .add(certificate)
                    .map_err(|error| refuse(format!("{}: {error}", path.display())))?;
            }
            if roots.is_empty() {
                return Err(refuse(format!("{} holds no certificate", path.display())));
            }
            roots
        }
    };
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("observability.otlp: {error}"))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// What `openqtt config check` adds for the exporter, once the files it names can be opened:
/// that the headers file reads as headers and the CA file holds certificates.
pub(crate) fn check(settings: &Settings) -> Vec<String> {
    let otlp = &settings.observability.otlp;
    if otlp.endpoint.is_none() {
        return Vec::new();
    }
    let mut problems = Vec::new();
    if let Some(file) = &otlp.headers_file
        && let Err(problem) = headers(file)
    {
        problems.push(problem);
    }
    if let Some(file) = &otlp.ca_file
        && let Err(problem) = tls(Some(file))
    {
        problems.push(problem);
    }
    problems
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead as _, BufReader, Read as _, Write as _};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use openqtt_observe::metrics::{Metrics, PacketType, SCOPE};
    use opentelemetry::metrics::MeterProvider as _;

    use super::*;

    /// A certificate authority made for these tests and nothing else.
    const TEST_CA: &str = "-----BEGIN CERTIFICATE-----
MIIBizCCATGgAwIBAgIUR8WHKnra/0AuqI6d+YeV42Nd434wCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPT3BlblFUVCB0ZXN0IENBMCAXDTI2MTAwMjA4NTM0NFoYDzIx
MjYwOTA4MDg1MzQ0WjAaMRgwFgYDVQQDDA9PcGVuUVRUIHRlc3QgQ0EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAAQjpYNrdOhK13Db4bMjiZ3JQLu3ORzHjwPcIkfC
DjbSLY+SfMNG7e7aCQxOOuD00hIkC4TZpjLbUvcj8PIoK2mPo1MwUTAdBgNVHQ4E
FgQU8iwXQPNDissxz+p1H27w/6gxLF8wHwYDVR0jBBgwFoAU8iwXQPNDissxz+p1
H27w/6gxLF8wDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiBfRiC/
Kj6L1kMd1/0sKYqLyB/D1PhAUdjPu/xNEhcsJgIhALdZ0cXSQXdoSWY1v/hmk+vP
/cQT6pr7sefcw4IDuWhM
-----END CERTIFICATE-----
";

    fn file(name: &str, contents: &[u8]) -> PathBuf {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        let call = CALLS.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "openqtt-telemetry-{}-{call}-{name}",
            std::process::id()
        ));
        std::fs::write(&path, contents).unwrap();
        path
    }

    /// One HTTP request, as the collector read it.
    struct Request {
        head: Vec<String>,
        body: Vec<u8>,
    }

    impl Request {
        fn header(&self, name: &str) -> Option<&str> {
            self.head.iter().skip(1).find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then(|| value.trim())
            })
        }
    }

    /// A collector that takes one request, answers 200 with an empty OTLP response, and hands
    /// the request back.
    fn collector() -> (String, JoinHandle<Request>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut head = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let line = line.trim_end().to_owned();
                if line.is_empty() {
                    break;
                }
                head.push(line);
            }
            let request = Request {
                head,
                body: Vec::new(),
            };
            let length = request
                .header("content-length")
                .map_or(0, |length| length.parse::<usize>().unwrap());
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\n\
                      content-length: 0\r\nconnection: close\r\n\r\n",
                )
                .unwrap();
            Request { body, ..request }
        });
        (format!("http://{address}"), handle)
    }

    fn settings_for(endpoint: &str) -> Settings {
        let mut settings = Settings::default();
        settings.cluster.node_name = Some("openqtt-edge-0".to_owned());
        settings.observability.otlp.endpoint = Some(endpoint.parse().unwrap());
        settings
    }

    #[test]
    fn without_an_endpoint_nothing_is_pushed() {
        assert!(meter_provider(&Settings::default()).unwrap().is_none());
        assert!(check(&Settings::default()).is_empty());
    }

    #[test]
    fn metrics_go_to_v1_metrics_as_protobuf_with_the_headers_of_the_file() {
        let (endpoint, collector) = collector();
        let headers = file(
            "headers",
            b"# for the collector\nAuthorization: Bearer opaque-token\n\nX-Scope-OrgID: fleet\n",
        );
        let mut settings = settings_for(&format!("{endpoint}/"));
        settings.observability.otlp.headers_file = Some(SecretFile::new(&headers));
        let provider = meter_provider(&settings).unwrap().unwrap();

        let metrics = Metrics::new(&provider.meter(SCOPE));
        metrics.packet_received(PacketType::Publish, 128);
        provider.force_flush().unwrap();
        let request = collector.join().unwrap();

        assert_eq!(request.head[0], "POST /v1/metrics HTTP/1.1");
        assert_eq!(
            request.header("content-type"),
            Some("application/x-protobuf")
        );
        assert_eq!(request.header("authorization"), Some("Bearer opaque-token"));
        assert_eq!(request.header("x-scope-orgid"), Some("fleet"));
        let body = String::from_utf8_lossy(&request.body);
        for expected in [
            "openqtt.packets.received",
            "openqtt.bytes.received",
            "publish",
            "openqtt-edge-0",
            env!("CARGO_PKG_VERSION"),
        ] {
            assert!(body.contains(expected), "`{expected}` is not in the export");
        }
    }

    #[test]
    fn an_unreachable_collector_does_not_stop_the_process_from_starting() {
        let closed = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let mut settings = settings_for(&endpoint);
        settings.observability.otlp.timeout = openqtt_config::Duration::from_secs(1);
        let provider = meter_provider(&settings).unwrap().unwrap();
        Metrics::new(&provider.meter(SCOPE)).session_taken_over();
        assert!(
            provider.force_flush().is_err(),
            "nothing could take the export"
        );
    }

    #[test]
    fn a_headers_file_is_read_line_by_line_and_a_bad_line_is_named_but_not_shown() {
        let good = file("good", b"a: 1\n\n# comment\nb:2\n  c : three words \n");
        let read = headers(&SecretFile::new(&good)).unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read["c"], "three words");
        assert!(read["a"].is_sensitive());

        for (contents, why) in [
            (&b"a: 1\nno colon hunter2\n"[..], "line 2 of"),
            (b"bad name: hunter2\n", "line 1 of"),
            (b"a:\n", "line 1 of"),
            (b"a: hunter2\x7f\n", "line 1 of"),
            (b"a: \xff\n", "is not UTF-8"),
        ] {
            let path = file("bad", contents);
            let error = headers(&SecretFile::new(&path)).unwrap_err();
            assert!(
                error.starts_with("observability.otlp.headers_file: "),
                "{error}"
            );
            assert!(error.contains(why), "{error}");
            assert!(!error.contains("hunter2"), "{error}");
        }
    }

    #[test]
    fn tls_trusts_the_ca_file_alone_or_else_the_built_in_roots() {
        tls(None).unwrap();
        tls(Some(&file("ca.pem", TEST_CA.as_bytes()))).unwrap();
        for (contents, why) in [
            (&b""[..], "holds no certificate"),
            (b"not a certificate\n", "holds no certificate"),
            (
                b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
                "observability.otlp.ca_file: ",
            ),
        ] {
            let error = tls(Some(&file("bad-ca.pem", contents))).unwrap_err();
            assert!(error.contains(why), "{error}");
        }
        let missing = std::env::temp_dir().join("openqtt-telemetry-missing.pem");
        let error = tls(Some(&missing)).unwrap_err();
        assert!(error.contains("cannot read"), "{error}");
    }

    #[test]
    fn check_reports_a_headers_file_or_a_ca_file_the_exporter_could_not_use() {
        let mut settings = settings_for("https://collector.example:4318");
        settings.observability.otlp.headers_file = Some(SecretFile::new(file("h", b"hunter2\n")));
        settings.observability.otlp.ca_file = Some(file("ca", b"nothing"));
        let problems = check(&settings);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].contains("line 1 of"), "{problems:?}");
        assert!(problems[1].contains("holds no certificate"), "{problems:?}");
    }
}
