use pyo3::prelude::{Python, PyModule, PyObject};
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
    elapsed_ms: Arc<AtomicU32>,
    msg_count: Arc<AtomicU32>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            elapsed_ms: Arc::new(AtomicU32::new(0)),
            msg_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

async fn counter_agent_1(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {

    let my_algo: PyObject= {
        let gil = Python::acquire_gil();
        let py = gil.python();
        PyModule::import(py, "my_py_algos").unwrap()
            .getattr("algo1_rust_version").unwrap()
            .into()
    };

    while let Some(msg) = stream.next().await {
        // This is used to measure the time the agent is active
        let now = Instant::now();
        // Read the incoming bytes as string
        let m = msg.unwrap();
        let strm = m
            .payload_view::<str>()
            .ok_or(CallystoError::GeneralError("Payload view failure".into()))??;

        // This block calls an "AI-Model" in python
        let arg = (strm,);
        let gil = Python::acquire_gil();
        let py = gil.python();
        let algo_res = my_algo
            .clone_ref(py)
            .getattr(py, "rust_wrapper").unwrap()
            .call1(py, arg).unwrap();

        let elapsed_ms: u32 = now.elapsed().as_millis() as u32;
        let state = ctx.state();
        let agent_time = state.elapsed_ms.fetch_add(elapsed_ms, Ordering::AcqRel);
        let msg_count = state.msg_count.fetch_add(1, Ordering::AcqRel);
        info!("AGENT1: Got data {}", strm);
        info!("AGENT1: Produced data {}", algo_res);
        info!("Total time elapsed in agents {}ms at {} messages", agent_time, msg_count);
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
        let elapsed_mus: u32 = now.elapsed().as_millis() as u32;
        let agent_time = state.elapsed_ms.fetch_add(elapsed_mus, Ordering::AcqRel);
        let msg_count = state.msg_count.fetch_add(1, Ordering::AcqRel);
        info!("AGENT1: Got data {}", _strm);
        info!("Total time elapsed in agents {}ms at {} messages", agent_time, msg_count);
    }

    Ok(())
}

fn main() {
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
