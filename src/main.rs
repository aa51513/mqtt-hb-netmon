use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet};
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{interval, sleep};
use chrono::{DateTime, Local};

#[derive(Parser, Debug)]
#[command(author, version, about = "Multi-Broker MQTT heartbeat latency monitor")]
struct Opt {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    /// 全局默认配置
    #[serde(default)]
    default: DefaultConfig,

    /// 统计报告间隔
    #[serde(default = "default_stats_interval")]
    stats_interval: String,

    /// Broker 列表
    brokers: Vec<BrokerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DefaultConfig {
    /// 默认心跳周期
    #[serde(default = "default_keepalive")]
    keepalive: String,

    /// 默认用户名
    #[serde(default)]
    username: Option<String>,

    /// 默认密码
    #[serde(default)]
    password: Option<String>,

    /// 默认 Clean Session
    #[serde(default = "default_clean_session")]
    clean_session: bool,
}

impl Default for DefaultConfig {
    fn default() -> Self {
        Self {
            keepalive: default_keepalive(),
            username: None,
            password: None,
            clean_session: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrokerConfig {
    /// Broker 地址，格式: host:port
    host: String,

    /// 心跳周期（可选，使用全局默认值）
    #[serde(default)]
    keepalive: Option<String>,

    /// 用户名（可选）
    #[serde(default)]
    username: Option<String>,

    /// 密码（可选）
    #[serde(default)]
    password: Option<String>,

    /// Clean Session（可选）
    #[serde(default)]
    clean_session: Option<bool>,

    /// 显示名称（可选）
    #[serde(default)]
    name: Option<String>,
}

fn default_keepalive() -> String {
    "5s".to_string()
}

fn default_stats_interval() -> String {
    "30s".to_string()
}

fn default_clean_session() -> bool {
    true
}

#[derive(Debug, Clone)]
struct Stats {
    count: u64,
    sum: u128,
    min: u128,
    max: u128,
    values: Vec<u128>,
    disconnect_count: u64,
    recent_disconnects: Vec<DateTime<Local>>,
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
}

#[tokio::main]
async fn main() {
    let opt = Opt::parse();

    // 读取配置文件
    let config = match load_config(&opt.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("❌ 无法读取配置文件 '{}': {}", opt.config, e);
            eprintln!("\n💡 提示：请创建配置文件，参考格式：");
            eprintln!("{}", example_config());
            return;
        }
    };

    if config.brokers.is_empty() {
        eprintln!("❌ 配置文件中没有定义任何 broker");
        return;
    }

    let stats_interval_duration = humantime::parse_duration(&config.stats_interval)
        .expect("invalid stats_interval duration");

    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║            MQTT Heartbeat Latency Monitor                      ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!("  Config file: {}", opt.config);
    println!("  Statistics interval: {:?}", stats_interval_duration);
    println!("  Brokers: {}", config.brokers.len());
    println!("────────────────────────────────────────────────────────────────\n");

    // 存储每个 broker 的统计信息
    let stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));

    let mut tasks = Vec::new();

    // 为每个 broker 启动独立任务
    for broker_config in config.brokers.clone() {
        let default_config = config.default.clone();
        let stats_clone = stats_map.clone();

        tasks.push(task::spawn(async move {
            monitor_broker(broker_config, default_config, stats_clone).await;
        }));
    }

    // 启动统计报告任务
    let stats_clone = stats_map.clone();
    let brokers = config.brokers.clone();
    tasks.push(task::spawn(async move {
        print_stats_periodically(stats_clone, brokers, stats_interval_duration).await;
    }));

    futures::future::join_all(tasks).await;
}

/// 加载配置文件
fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

/// 示例配置
fn example_config() -> &'static str {
    r#"
# 全局默认配置
[default]
keepalive = "5s"           # 默认心跳间隔
# username = "default_user" # 默认用户名（可选）
# password = "default_pass" # 默认密码（可选）
clean_session = true       # 默认 Clean Session

# 统计报告间隔
stats_interval = "30s"

# Broker 列表
[[brokers]]
host = "localhost:1883"
name = "Local Broker"
# keepalive = "10s"        # 覆盖默认值（可选）
# username = "user1"       # 覆盖默认值（可选）
# password = "pass1"       # 覆盖默认值（可选）

[[brokers]]
host = "broker.emqx.io:1883"
name = "EMQX Public"
keepalive = "15s"

[[brokers]]
host = "test.mosquitto.org:1883"
name = "Mosquitto Test"
username = "testuser"
password = "testpass"
clean_session = false
"#
}

/// 定期打印统计信息
async fn print_stats_periodically(
    stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
    brokers: Vec<BrokerConfig>,
    interval_duration: Duration,
) {
    let mut ticker = interval(interval_duration);

    loop {
        ticker.tick().await;

        let stats = stats_map.lock().await;

        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║                    Statistics Report                           ║");
        println!("╚═══════════════════════════════════════════════════════════════╝");

        for broker_config in &brokers {
            let broker_key = get_broker_key(broker_config);
            let display_name = broker_config.name.as_ref()
                .unwrap_or(&broker_config.host);

            if let Some(s) = stats.get(&broker_key) {
                if s.count > 0 || s.disconnect_count > 0 {
                    println!("\n📊 Broker: {} ({})", display_name, broker_config.host);

                    if s.count > 0 {
                        println!("   ├─ Samples: {}", s.count);
                        println!("   ├─ Min RTT: {} ms", s.min);
                        println!("   ├─ Max RTT: {} ms", s.max);
                        println!("   ├─ Avg RTT: {:.2} ms", s.avg());
                        println!("   ├─ Std Dev: {:.2} ms (jitter)", s.std_dev());
                    } else {
                        println!("   ├─ No RTT data yet");
                    }

                    println!("   ├─ Disconnects: {}", s.disconnect_count);

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
                    println!("\n📊 Broker: {} ({}) - No data yet", display_name, broker_config.host);
                }
            }
        }

        println!("\n────────────────────────────────────────────────────────────────");
    }
}

/// 获取 broker 的唯一标识
fn get_broker_key(broker_config: &BrokerConfig) -> String {
    broker_config.host.clone()
}

/// 独立任务：监测单个 broker 的 ping 往返延迟
async fn monitor_broker(
    broker_config: BrokerConfig,
    default_config: DefaultConfig,
    stats_map: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
) {
    let (host, port) = parse_host_port(&broker_config.host);
    let broker_key = get_broker_key(&broker_config);

    // 获取实际配置值（优先使用 broker 自己的配置，否则使用默认配置）
    let keepalive_str = broker_config.keepalive.as_ref()
        .unwrap_or(&default_config.keepalive);
    let keepalive = humantime::parse_duration(keepalive_str)
        .expect("invalid keepalive duration");

    let username = broker_config.username.as_ref()
        .or(default_config.username.as_ref());
    let password = broker_config.password.as_ref()
        .or(default_config.password.as_ref());
    let clean_session = broker_config.clean_session
        .unwrap_or(default_config.clean_session);

    let client_id = format!("mqtt-hb-{}-{}", host, rand::random::<u16>());
    let mut mqtt = MqttOptions::new(client_id, host, port);
    mqtt.set_keep_alive(keepalive);
    mqtt.set_clean_session(clean_session);

    // 设置认证信息
    if let (Some(user), Some(pass)) = (username, password) {
        mqtt.set_credentials(user, pass);
    }

    let (client, mut eventloop) = AsyncClient::new(mqtt, 10);

    // 初始化该 broker 的统计
    {
        let mut stats = stats_map.lock().await;
        stats.insert(broker_key.clone(), Stats::new());
    }

    let mut last_ping_time: Option<Instant> = None;
    let mut reconnects = 0u64;

    let display_name = broker_config.name.as_ref()
        .map(|n| format!("{} ({})", n, broker_config.host))
        .unwrap_or_else(|| broker_config.host.clone());

    println!("🔌 [{}] Connecting... (keepalive: {:?}, auth: {})",
             display_name, keepalive,
             if username.is_some() { "yes" } else { "no" });

    loop {
        match eventloop.poll().await {
            Ok(event) => match event {
                Event::Outgoing(Outgoing::PingReq) => {
                    last_ping_time = Some(Instant::now());
                }
                Event::Incoming(Packet::PingResp) => {
                    if let Some(start) = last_ping_time {
                        let rtt = start.elapsed().as_millis();
                        println!("💓 [{}] PINGRESP RTT = {} ms", display_name, rtt);

                        let mut stats = stats_map.lock().await;
                        if let Some(s) = stats.get_mut(&broker_key) {
                            s.add(rtt);
                        }
                    }
                }
                Event::Incoming(Packet::ConnAck(_)) => {
                    println!("✅ [{}] Connected successfully", display_name);
                }
                _ => {}
            },

            Err(e) => {
                reconnects += 1;
                let disconnect_time = Local::now();

                println!("❌ [{}] EventLoop error: {} → reconnecting... (#{}) at {}",
                         display_name, e, reconnects,
                         disconnect_time.format("%Y-%m-%d %H:%M:%S%.3f"));

                {
                    let mut stats = stats_map.lock().await;
                    if let Some(s) = stats.get_mut(&broker_key) {
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
