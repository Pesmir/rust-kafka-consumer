use pyo3::prelude::{Python, PyModule};
use callysto::futures::StreamExt;
use callysto::kafka::cconsumer::CStream;
use callysto::kafka::enums::OffsetReset;
use callysto::prelude::message::*;
use callysto::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tracing::info;

#[derive(Clone)]
struct SharedState {
    elapsed_us: Arc<AtomicU32>,
    msg_count: Arc<AtomicU32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            elapsed_us: Arc::new(AtomicU32::new(0)),
            msg_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

async fn counter_agent_1(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    pyo3::prepare_freethreaded_python();
    let my_module_path = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/my_py_algos.py"));

    while let Some(msg) = stream.next().await {
        // Read the incoming bytes as string
        let m = msg.unwrap();
        let strm = m
            .payload_view::<str>()
            .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;

        let state = ctx.state();

        let gil = Python::acquire_gil();
        let py = gil.python();
        let my_algo1 = PyModule::from_code(py, &my_module_path, "", "")
            .unwrap()
            .getattr("algo1_rust_version").unwrap()
            .getattr("rust_wrapper").unwrap();
        let now = Instant::now();
        let arg = (strm,);
        let algo_res = my_algo1.call1(arg).unwrap();
        

        let elapsed_mus: u32 = now.elapsed().as_micros() as u32;
        let agent_time = state.elapsed_us.fetch_add(elapsed_mus, Ordering::AcqRel);
        let msg_count = state.msg_count.fetch_add(1, Ordering::AcqRel);
        info!("AGENT1: Got data {}", strm);
        info!("AGENT1: Produced data {}", algo_res);
        info!("Total time elapsed in agents {}us at {} messages", agent_time, msg_count);
    }

    Ok(())
}

async fn counter_agent_2(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    while let Some(msg) = stream.next().await {
        let now = Instant::now();
        // Read the incoming bytes as string
        let m = msg.unwrap();
        let _strm = m
            .payload_view::<str>()
            .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;

        let state = ctx.state();
        let elapsed_mus: u32 = now.elapsed().as_micros() as u32;
        let agent_time = state.elapsed_us.fetch_add(elapsed_mus, Ordering::AcqRel);
        let msg_count = state.msg_count.fetch_add(1, Ordering::AcqRel);
        info!("AGENT1: Got data {}", _strm);
        info!("Total time elapsed in agents {}us at {} messages", agent_time, msg_count);
    }

    Ok(())
}

fn main() {
    // Throughput: 278.472900390625 MB/sec
    let mut config = Config::default();
    config.kafka_config.auto_offset_reset = OffsetReset::Earliest;

    let mut app = Callysto::with_state(SharedState::new());

    app.with_name("double-agent");
    app.agent(
        "counter_agent_1",
        app.topic("double-agent-1"),
        counter_agent_1,
    )
    .agent(
        "counter_agent_2",
        app.topic("double-agent-2"),
        counter_agent_2,
    );

    app.run();
}
