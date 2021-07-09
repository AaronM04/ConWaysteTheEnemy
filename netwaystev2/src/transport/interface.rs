use crate::common::Endpoint;
use crate::filter::Packet;

use std::time::Duration;

// https://serverfault.com/questions/645890/tcpdump-truncates-to-1472-bytes-useful-data-in-udp-packets-during-the-capture/645892#645892
pub const UDP_MTU_SIZE: usize = 1472;

/// Filter layer sends these commands to the Transport Layer to manage endpoints and their packets
#[derive(Debug)]
pub enum TransportCmd {
    NewEndpoint {
        endpoint: Endpoint,
        timeout:  Duration,
    },
    SendPackets {
        endpoint:     Endpoint,
        packet_infos: Vec<PacketSettings>,
        packets:      Vec<Packet>,
    },
    DropEndpoint {
        endpoint: Endpoint,
    },
    DropPacket {
        endpoint: Endpoint,
        tid:      usize,
    },
    CancelTransmitQueue {
        endpoint: Endpoint,
    },
}

/// Transport layer sends these response codes for each Filter layer command (see `TransportCmd`)
#[derive(Debug)]
pub enum TransportRsp {
    Accepted,
    BufferFull,
    ExceedsMtu {
        tid: usize,
    },
    EndpointError {
        error: anyhow::Error,
    },
    SendPacketsLengthMismatch,
}

/// Used by the Transport layer to inform the Filter layer of a packet or endpoint event
#[derive(Debug)]
pub enum TransportNotice {
    /// Here is the received packet for this endpoint
    PacketDelivery {
        endpoint: Endpoint,
        packet: Packet,
    },

    /// The maximum time since a packet was received from this endpoint was exceeded.
    EndpointTimeout {
        endpoint: Endpoint,
    },
}

/// Used by the Filter layer to inform the Transport layer of packet settings
#[derive(Debug)]
pub struct PacketSettings {
    /// Transmit ID, a unique identifier used to sync packet transactions between the filter and Transport layers
    pub tid:            usize,
    /// The length of time in between each retry attempt
    pub retry_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum EndpointDataError {
    #[error("{endpoint:?} not found in transmit queue: {message}")]
    EndpointNotFound {
        endpoint:   Endpoint,
        message:    String,
    },
    #[error("{endpoint:?} entry exists in transmit queue: {entry_found:?}")]
    EndpointExists {
        endpoint:    Endpoint,
        entry_found: Endpoint,
    },
    #[error("Transmit ID {tid} not found for {endpoint:?} in Transmit queue")]
    TransmitIDNotFound { endpoint: Endpoint, tid: usize },
    #[error("Could not remove packet at index {index} from transmit queue with tid {tid} for {endpoint:?}")]
    PacketRemovalFailure {
        endpoint:   Endpoint,
        tid:        usize,
        index:      usize,
    },
    #[error("{endpoint:?} could not be dropped : {message}")]
    EndpointDropFailed { endpoint: Endpoint, message: String },
}