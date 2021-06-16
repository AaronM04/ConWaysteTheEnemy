use crate::filter::Packet;

use bincode::{deserialize, serialize};
use bytes::{Buf, BytesMut};
pub use tokio_util::codec::LinesCodec;
use tokio_util::codec::{Decoder, Encoder};

use std::io;
pub struct NetwaystePacketCodec;

impl Decoder for NetwaystePacketCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match deserialize(src) {
            Ok(decoded) => {
                let pkt: Packet = decoded;
                match bincode::serialized_size(&pkt) {
                    Ok(length) => src.advance(length as usize),
                    Err(err) => {
                        // Something went horribly wrong if we were unable to serialize something we just deserialized.
                        // Clear the buffer and restart the decoder by returning an error.
                        src.clear();
                        return Err(io::Error::new(io::ErrorKind::InvalidData, err));
                    }
                }
                Ok(Some(pkt))
            }
            Err(_) => Ok(None),
        }
    }
}

impl Encoder<Packet> for NetwaystePacketCodec {
    type Error = io::Error;

    fn encode(&mut self, packet: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded: Vec<u8> = serialize(&packet).unwrap();
        dst.extend_from_slice(&encoded[..]);
        Ok(())
    }
}
