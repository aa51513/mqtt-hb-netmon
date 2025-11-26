use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{interval, sleep};
use chrono::{DateTime, Local};

#[derive(Parser, Debug)]
#[command(author, version, about = "Multi-Broker MQTT heartbeat latency monitor")]
struct Opt {
    /// 多个 broker，格式: host:port
    #[arg(short, long)]
    broker: Vec<String>,

    /// 心跳周期（keepalive）
    #[arg(short, long, default_value = "5s")]
    keepalive: String,

    /// 统计报告间隔
    #[arg(short, long, default_value = "30s")]
    stats_interval: String,
}

#[derive(Debug, Clone)]
struct Stats {
    count: u64,
    sum: u128,
    min: u128,
    max: u128,
    values: Vec<u128>, // 用于计算标准差
    disconnect_count: u64, // 断开连接次数
    recent_disconnects: Vec<DateTime<Local>>, // 最近5次断开时间
}

impl Stats {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0,
            min: u128::MAX,
            max: 0,
            values: Vec::new(),
            disconnect_count: 0,
            recent_disconnects: Vec::new(),
        }
    }

    fn add(&mut self, rtt: u128) {
        self.count += 1;
        self.sum += rtt;
        self.min = self.min.min(rtt);
        self.max = self.max.max(rtt);
        self.values.push(rtt);
    }

    fn add_disconnect(&mut self) {
        self.disconnect_count += 1;
        let now = Local::now();

        // 保持最近5次断开时间
        self.recent_disconnects.push(now);
        if self.recent_disconnects.len() > 5 {
            self.recent_disconnects.remove(0);
        }
    }

    fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }

    fn std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let avg = self.avg();
        let variance: f64 = self.values.iter()
            .map(|&v| {
                let diff = v as f64 - avg;
                diff * diff
            })
            .sum::<f64>() / self.count as f64;
        variance.sqrt()
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[tokio::main]
async fn main() {
    let opt = Opt::parse();

    if opt.broker.is_empty() {
        eprintln!("需要至少一个 --broker <host:port>");
        return;
    }

    let keepalive = humantime::parse_duration(&opt.keepalive)
        .expect("invalid keepalive duration");

    let stats_interval_duration = humantime::parse_duration(&opt.stats_interval)
        .expect("invalid stats_interval duration");

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║            MQTT Heartbeat Latency Monitor                      ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!("  Heartbeat interval: {:?}", keepalive);
    println!("  Statistics interval: {:?}", stats_interval_duration);
    println!("  Brokers: {:?}", opt.broker);
    println!("────────────────────────────────────────────────────────────────\n");

    // 存储每个 broker 的统计信息
    let stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    // 为每个 broker 启动独立任务
    let mut tasks = Vec::new();

    for broker in opt.broker.clone() {
        let ka = keepalive;
        let stats_clone = stats_map.clone();

        tasks.push(task::spawn(async move {
            monitor_broker(broker, ka, stats_clone).await;
        }));
    }

    // 启动统计报告任务
    let stats_clone = stats_map.clone();
    let brokers = opt.broker.clone();
    tasks.push(task::spawn(async move {
        print_stats_periodically(stats_clone, brokers, stats_interval_duration).await;
    }));

    // 等待所有任务运行（不会退出）
    futures::future::join_all(tasks).await;
}

/// 定期打印统计信息
async fn print_stats_periodically(
    stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
    brokers: Vec<String>,
    interval_duration: Duration,
) {
    let mut ticker = interval(interval_duration);

    loop {
        ticker.tick().await;

        let stats = stats_map.lock().await;

        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║                    Statistics Report                           ║");
        println!("╚═══════════════════════════════════════════════════════════════╝");

        for broker in &brokers {
            if let Some(s) = stats.get(broker) {
                if s.count > 0 || s.disconnect_count > 0 {
                    println!("\n📊 Broker: {}", broker);

                    // RTT 统计
                    if s.count > 0 {
                        println!("   ├─ Samples: {}", s.count);
                        println!("   ├─ Min RTT: {} ms", s.min);
                        println!("   ├─ Max RTT: {} ms", s.max);
                        println!("   ├─ Avg RTT: {:.2} ms", s.avg());
                        println!("   ├─ Std Dev: {:.2} ms (jitter)", s.std_dev());
                    } else {
                        println!("   ├─ No RTT data yet");
                    }

                    // 断开连接统计
                    println!("   ├─ Disconnects: {}", s.disconnect_count);

                    // 最近5次断开时间
                    if !s.recent_disconnects.is_empty() {
                        println!("   └─ Recent disconnects:");
                        for (i, dt) in s.recent_disconnects.iter().enumerate() {
                            let prefix = if i == s.recent_disconnects.len() - 1 {
                                "      └─"
                            } else {
                                "      ├─"
                            };
                            println!("{} {}", prefix, dt.format("%Y-%m-%d %H:%M:%S%.3f"));
                        }
                    } else {
                        println!("   └─ No disconnects recorded");
                    }
                } else {
                    println!("\n📊 Broker: {} - No data yet", broker);
                }
            }
        }

        println!("\n────────────────────────────────────────────────────────────────");
    }
}

/// 独立任务：监测单个 broker 的 ping 往返延迟
async fn monitor_broker(
    broker: String,
    keepalive: Duration,
    stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
) {
    let (host, port) = parse_host_port(&broker);

    let client_id = format!("mqtt-hb-{}-{}", host, rand::random::<u16>());
    let mut mqtt = MqttOptions::new(client_id, host, port);
    mqtt.set_keep_alive(keepalive);

    let (client, mut eventloop) = AsyncClient::new(mqtt, 10);

    // 初始化该 broker 的统计
    {
        let mut stats = stats_map.lock().await;
        stats.insert(broker.clone(), Stats::new());
    }

    let mut last_ping_time: Option<Instant> = None;
    let mut reconnects = 0u64;

    println!("🔌 [{}] Connecting...", broker);

    loop {
        match eventloop.poll().await {
            Ok(event) => match event {
                Event::Outgoing(Outgoing::PingReq) => {
                    last_ping_time = Some(Instant::now());
                }
                Event::Incoming(Packet::PingResp) => {
                    if let Some(start) = last_ping_time {
                        let rtt = start.elapsed().as_millis();
                        println!("💓 [{}] PINGRESP RTT = {} ms", broker, rtt);

                        // 更新统计
                        let mut stats = stats_map.lock().await;
                        if let Some(s) = stats.get_mut(&broker) {
                            s.add(rtt);
                        }
                    }
                }
                Event::Incoming(Packet::ConnAck(_)) => {
                    println!("✅ [{}] Connected successfully", broker);
                }
                _ => {}
            },

            Err(e) => {
                reconnects += 1;
                let disconnect_time = Local::now();

                println!("❌ [{}] EventLoop error: {} → reconnecting... (#{}) at {}",
                         broker, e, reconnects, disconnect_time.format("%Y-%m-%d %H:%M:%S%.3f"));

                // 记录断开连接
                {
                    let mut stats = stats_map.lock().await;
                    if let Some(s) = stats.get_mut(&broker) {
                        s.add_disconnect();
                    }
                }

                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// 解析 host:port
fn parse_host_port(s: &str) -> (&str, u16) {
    let mut iter = s.split(':');
    let host = iter.next().unwrap_or("127.0.0.1");
    let port = iter.next().unwrap_or("1883").parse::<u16>().unwrap_or(1883);
    (host, port)
}
