use crate::common::Endpoint;
use crate::filter::Packet;

use std::time::Duration;

// https://serverfault.com/questions/645890/tcpdump-truncates-to-1472-bytes-useful-data-in-udp-packets-during-the-capture/645892#645892
pub const UDP_MTU_SIZE: usize = 1472;

#[derive(Debug, Copy, Clone)]
pub enum TransportQueueKind {
    Transmit,
    Receive,
    Meta,
}

/// Filter layer sends these commands to the Transport Layer to manage endpoints and their packets
#[derive(Debug)]
pub enum TransportCmd {
    NewEndpoint {
        endpoint: Endpoint,
        timeout:  Duration,
    },
    GetQueueCount {
        endpoint: Endpoint,
        kind:     TransportQueueKind,
    },
    TakeReceivedPackets {
        endpoint: Endpoint,
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
    TakenPackets {
        endpoint: Endpoint,
        packets: Vec<Packet>,
    },
    QueueCount {
        endpoint: Endpoint,
        kind:     TransportQueueKind,
        count:    usize,
    },
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
    /// There are packets available on this endpoint
    PacketsAvailable {
        endpoint: Endpoint,
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
    #[error("{endpoint:?} not found in {queue_kind:?} queue: {message}")]
    EndpointNotFound {
        queue_kind: TransportQueueKind,
        endpoint:   Endpoint,
        message:    String,
    },
    #[error("{endpoint:?} entry exists in {queue_kind:?} queue: {entry_found:?}")]
    EndpointExists {
        queue_kind:  TransportQueueKind,
        endpoint:    Endpoint,
        entry_found: Endpoint,
    },
    #[error("Transmit ID {tid} not found for {endpoint:?} in Transmit queue")]
    TransmitIDNotFound { endpoint: Endpoint, tid: usize },
    #[error("Could not remove packet at index {index} from {queue_kind:?} queue with tid {tid} for {endpoint:?}")]
    PacketRemovalFailure {
        queue_kind: TransportQueueKind,
        endpoint:   Endpoint,
        tid:        usize,
        index:      usize,
    },
    #[error("Invalid queue-kind {kind:?}")]
    InvalidQueueKind { kind: TransportQueueKind },
    #[error("{endpoint:?} could not be dropped : {message}")]
    EndpointDropFailed { endpoint: Endpoint, message: String },
}
