use std::collections::HashMap;
use std::io::ErrorKind;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use config::Config;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::{task, time};
use tokio::sync::RwLock;
use std::sync::Arc;
use tokio::time::Instant;
use log::{debug, error, trace, warn};
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;
use axum::extract::State;
use axum::Router;
use axum::routing::get;

type SpeedTestData = HashMap<String, (String, u128, u128)>;

const MAX_TEST_SIZE: u64 = 268_435_356;

#[derive(serde::Deserialize, Default)]
struct SpeedTestConfig {
    pub my_name: String,
    pub other_instances: Vec<String>,
    pub listen_port: u16,
    pub test_size: u64,
    pub test_freq: u64,
    pub bind_addr: String,
    pub prom_listen_port: u16,
    pub log_file: String
}

async fn read_ok<T: AsyncReadExt + Unpin>(stream: &mut T, addr: &str) -> Result<(), Box<dyn Error>> {
    let mut ok_vec = vec![0u8; 2];
    stream.read_exact(&mut ok_vec).await?;

    if String::from_utf8(ok_vec).unwrap().eq("OK") {
        Ok(())
    } else {
        error!("Bad response from server {}", addr);
        Err(Box::new(io::Error::new(ErrorKind::InvalidData, "Bad data from client")))
    }
}

async fn run_speed_test(other_inst: &str, test_size: u64, data: &RwLock<SpeedTestData>) -> Result<(), Box<dyn Error>> {
    trace!("Testing speed to {}", other_inst);

    let socket_raw = TcpStream::connect(other_inst).await?;
    let mut socket = BufStream::new(socket_raw);

    let mut name_buf: Vec<u8> = vec![];
    socket.read_until(0u8, &mut name_buf).await?;

    let name_raw = String::from_utf8(name_buf).unwrap();
    let name = &name_raw[0..name_raw.len() - 1];
    trace!("Got instance name {}", name);

    socket.write_u64(test_size).await?;
    socket.flush().await?;

    read_ok(&mut socket, other_inst).await?;

    let test_data = vec![0b11100110u8; test_size as usize];

    trace!("Writing {} bytes", test_size);

    let start = Instant::now();
    socket.write_all(&test_data).await?;
    socket.flush().await?;

    read_ok(&mut socket, other_inst).await?;

    let duration = start.elapsed().as_millis();

    trace!("Done in {}ms", duration);

    let time = SystemTime::now();
    let time_millis = time.duration_since(UNIX_EPOCH).unwrap().as_millis();

    let mut write_data = data.write().await;
    write_data.insert(String::from(other_inst), (String::from(name), start.elapsed().as_millis(), time_millis));

    Ok(())
}

async fn handle_client(conf: &SpeedTestConfig, data: &RwLock<SpeedTestData>) -> Result<(), Box<dyn Error>> {
    for other_inst in &conf.other_instances {
        match run_speed_test(other_inst.as_str(), conf.test_size, &data).await {
            Err(e) => error!("{}", e),
            _ => {}
        };
    }

    Ok(())
}

async fn handle_server(conf: &SpeedTestConfig, socket_raw: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut socket = BufStream::new(socket_raw);

    let name_bytes = conf.my_name.as_bytes();
    socket.write_all(name_bytes).await?;
    socket.write_u8(0).await?;

    socket.flush().await?;

    let test_size = socket.read_u64().await?;
    trace!("Expecting test size {}", test_size);

    if test_size > MAX_TEST_SIZE {
        warn!("Requested test size {} > {}", test_size, MAX_TEST_SIZE);
        return Err(Box::new(io::Error::new(ErrorKind::InvalidData, "Test size too large")));
    }

    socket.write_all("OK".as_bytes()).await?;
    socket.flush().await?;

    let mut buf = vec![0u8; test_size as usize];
    socket.read_exact(&mut buf).await?;

    socket.write_all("OK".as_bytes()).await?;
    socket.flush().await?;

    trace!("Test complete");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let settings = Config::builder()
        .add_source(config::File::with_name("topology"))
        .build()
        .unwrap();

    let config = Arc::new(settings.try_deserialize::<SpeedTestConfig>().unwrap());

    let log_file = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(&config.log_file)
        .await?;

    structured_logger::Builder::with_level("trace")
        .with_default_writer(structured_logger::async_json::new_writer(log_file))
        .init();

    let data: Arc<RwLock<SpeedTestData>> = Arc::new(RwLock::new(HashMap::new()));

    let mut handles = vec![];

    let config1 = Arc::clone(&config);
    handles.push(task::spawn(async move {
        loop {
            let listener = TcpListener::bind(format!("{}:{}", &config1.bind_addr, &config1.listen_port)).await;

            if let Err(e) = listener {
                error!("{}", e);
                continue;
            }

            match listener.unwrap().accept().await {
                Ok((socket, addr)) => {
                    debug!("Connection received: {}", &addr);
                    if let Err(e) = handle_server(&config1, socket).await {
                        error!("Handle server: {}", e);
                    }
                },
                Err(e) => error!("{}", e)
            };
        }
    }));

    let config2 = Arc::clone(&config);
    let data1 = Arc::clone(&data);
    handles.push(task::spawn( async move {
        let mut interval = time::interval(Duration::from_secs(config2.test_freq * 60));

        loop {
            interval.tick().await;
            debug!("Starting speed tests");

            if let Err(e) = handle_client(&config2, &data1).await {
                error!("Handle client: {}", e)
            }
        }
    }));

    let data2 = Arc::clone(&data);
    let http_port = config.prom_listen_port;
    let test_size = config.test_size;
    let bind_address = config.bind_addr.clone();
    let my_name = config.my_name.clone();
    handles.push(task::spawn(async move {
        async fn show_metrics(State((data_arc, test_size, my_name)): State<(Arc<RwLock<SpeedTestData>>, u64, String)>) -> String {
            debug!("Metrics requested");

            let map = data_arc.read().await;

            let metrics = map.values().map(|(name, duration, timestamp)| {
                let bps = (test_size as f64 / (*duration as f64 / 1000.0)) as u128 * 8u128;
                format!("upload_speed_bits_per_second{{from_node=\"{}\",to_node=\"{}\"}} {} {}", my_name, name, bps, timestamp)
            }).collect::<Vec<String>>().join("\n");

            format!("# HELP upload_speed_bits_per_second Last upload speed test to node in bits per second.
# TYPE upload_speed_bits_per_second gauge
{}\n", metrics)
        }

        let app = Router::new()
            .route("/metrics", get(show_metrics))
            .with_state((data2, test_size, my_name));

        let addr = SocketAddr::from_str(&*format!("{}:{}", bind_address, http_port)).unwrap();
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }));

    futures::future::join_all(handles).await;

    Ok(())
}
