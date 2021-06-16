use super::ping::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum FilterMode {
    Client,
    Server,
}

////////////////////// Data model ////////////////////////
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum RequestAction {
    None, // never actually sent

    /* These actions do not require a user to be logged in to the server */
    Connect {
        name:           String,
        client_version: String,
    },

    /* All actions below require a log-in via a Connect request */
    Disconnect,
    KeepAlive {
        latest_response_ack: u64,
    }, // Send latest response ack on each heartbeat
    ListPlayers,
    ChatMessage {
        message: String,
    },
    ListRooms,
    NewRoom {
        room_name: String,
    },
    JoinRoom {
        room_name: String,
    },
    LeaveRoom,
    // TODO: add support ("auto_match" bool key, see issue #101)
    SetClientOptions {
        key: String,
        /* PR_GATE add in later
        value: Option<ClientOptionValue>,
        */
    },
    // TODO: add support
    // Draw the specified RLE Pattern with upper-left cell at position x, y.
    DropPattern {
        x:       i32,
        y:       i32,
        pattern: String,
    },
    // TODO: add support (also need it in the ggez client)
    // Clear all cells in the specified region not belonging to other players. No part of this
    // region may be outside the player's writable region.
    ClearArea {
        x: i32,
        y: i32,
        w: u32,
        h: u32,
    },
}

// server response codes -- mostly inspired by https://en.wikipedia.org/wiki/List_of_HTTP_status_codes
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum ResponseCode {
    // success - these are all 200 in HTTP
    // TODO: Many of these should contain the sequence number being acknowledged
    OK, // 200 no data
    LoggedIn {
        cookie:         String,
        server_version: String,
    }, // player is logged in -- (cookie, server version)
    JoinedRoom {
        room_name: String,
    }, // player has joined the room
    LeaveRoom, // player has left the room
    PlayerList {
        players: Vec<String>,
    }, // list of players in room or lobby
    RoomList {
        rooms: Vec<RoomList>,
    }, // list of rooms and their statuses

    // errors
    BadRequest {
        error_msg: String,
    }, // 400 unspecified error that is client's fault
    Unauthorized {
        error_msg: String,
    }, // 401 not logged in
    TooManyRequests {
        error_msg: String,
    }, // 429
    ServerError {
        error_msg: String,
    }, // 500
    NotConnected {
        error_msg: String,
    }, // no equivalent in HTTP due to handling at lower (TCP) level

    // Misc.
    KeepAlive, // Server's heart is beating
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Packet {
    Request {
        // sent by client
        sequence:     u64,
        response_ack: Option<u64>, // Next expected  sequence number the Server responds with to the Client.
        // Stated differently, the client has seen Server responses from 0 to response_ack-1.
        cookie:       Option<String>, // present if and only if action != connect
        action:       RequestAction,
    },
    Response {
        // sent by server in reply to client
        sequence:    u64,
        request_ack: Option<u64>, // most recent request sequence number received
        code:        ResponseCode,
    },
    Update {
        // Usually in-game: sent by server.
        // All of these except ping are reset to new values upon joining a room and cleared upon
        // leaving. Also note that the server may not send all GameUpdates or BroadcastChatMessages
        // in a single packet, since it could exceed the MTU.
        // TODO: limit chats and game_updates based on MTU!
        chats:           Vec<BroadcastChatMessage>, // All non-acknowledged chats are sent each update
        game_update_seq: Option<u64>,
        /* PR_GATE add in later
        game_updates:    Vec<GameUpdate>, // Information pertaining to a game tick update.
        universe_update: UniUpdate,       // TODO: add support
        */
        ping:            PingPong, // Used for server-to-client latency measurement (no room needed)
    },
    UpdateReply {
        // in-game: sent by client in reply to server
        cookie:               String,
        last_chat_seq:        Option<u64>, // sequence number of latest chat msg. received from server
        last_game_update_seq: Option<u64>, // seq. number of latest game update from server
        last_full_gen:        Option<u64>, // generation number client is currently at
        /* PR_GATE add in later
        partial_gen:          Option<GenPartInfo>, // partial gen info, if some but not all GenStateDiffParts recv'd
        */
        pong:                 PingPong, // Used for server-to-client latency measurement
    },
    GetStatus {
        ping: PingPong, // Used for client-to-server latency measurement
    },
    Status {
        pong:           PingPong, // used for client-to-server latency measurement
        server_version: String,
        player_count:   u64,
        room_count:     u64,
        server_name:    String,
        // TODO: max players?
    }, // Provide basic server information to the requester
}

impl Default for Packet {
    fn default() -> Self {
        Packet::Request {
            sequence:     0,
            response_ack: None,
            cookie:       None,
            action:       RequestAction::None,
        }
    }
}




// chat messages sent from server to all clients other than originating client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BroadcastChatMessage {
    pub chat_seq:    Option<u64>, // Some(<number>) when sent to clients (starts at 0 for first
    // chat message sent to this client in this room); None when
    // internal to server
    pub player_name: String,
    pub message:     String, // should not contain newlines
}

impl BroadcastChatMessage {
    #[allow(unused)]
    pub fn new(sequence: u64, name: String, msg: String) -> BroadcastChatMessage {
        BroadcastChatMessage {
            chat_seq:    Some(sequence),
            player_name: name,
            message:     msg,
        }
    }

    fn sequence_number(&self) -> u64 {
        if let Some(v) = self.chat_seq {
            v
        } else {
            0
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RoomList {
    pub room_name:    String,
    pub player_count: u8,
    // TODO: add support
    pub in_progress:  bool,
}
