extern crate color_backtrace;
extern crate env_logger;
#[macro_use]
extern crate log;

use netwaystev2::common::Endpoint;
use netwaystev2::filter::{Filter, FilterMode};
use netwaystev2::transport::Transport;
use netwaystev2::transport::{PacketSettings, TransportCmd};

use anyhow::Result;
use std::io::Write;
use std::time::Duration;
use tokio::time::sleep;

use chrono::Local;

#[tokio::main]
async fn main() -> Result<()> {
    color_backtrace::install();

    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{:5}] - {}",
                Local::now().format("%a %Y-%m-%d %H:%M:%S%.6f"),
                record.level(),
                record.args(),
            )
        })
        .filter_level(log::LevelFilter::max())
        .target(env_logger::Target::Stdout)
        .init();

    // The interesting stuff starts here!

    // Create the lowest (Transport) layer, returning the layer itself plus three channel halves
    // (one outgoing and two incoming) for communicating with it.
    let (mut transport, transport_cmd_tx, transport_rsp_rx, transport_notice_rx) = Transport::new(None, None)?;

    // Start the transport's task in the background
    tokio::spawn(async move { transport.run().await });
    info!("Transport initialized!");

    // Send a fake "NewEndpoint" command to the transport layer to kick things off
    transport_cmd_tx
        .send(TransportCmd::NewEndpoint {
            endpoint: Endpoint("127.0.0.1:2017".parse().unwrap()),
            timeout:  Duration::new(5, 0), // PR_GATE make this configurable and reasonably valued
        })
        .await?;

    // Create the second lowest (Filter) layer, passing in the channel halves that connect to the
    // layer below it
    let mut filter = Filter::new(
        transport_cmd_tx,
        transport_rsp_rx,
        transport_notice_rx,
        FilterMode::Server,
    );

    // Start the filter's task in the background
    tokio::spawn(async move { filter.run().await });
    info!("Filter initialized!");

    // Sleep for a really really long time
    // TODO: wait for shutdown signal, and then coordinate shutdown with all the layers
    sleep(Duration::from_secs(u64::max_value())).await;
    Ok(())
}
