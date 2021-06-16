mod filter;
mod interface;
mod ping;
mod sortedbuffer;
use sortedbuffer::SequencedMinHeap;
use interface::{ResponseCode, RequestAction};

pub use filter::Filter;
pub use interface::{FilterMode, Packet};

enum EndpointData {
    OtherEndClient {
        request_actions: SequencedMinHeap<RequestAction>,
    },
    OtherEndServer {
        response_codes: SequencedMinHeap<ResponseCode>,
    },
}
