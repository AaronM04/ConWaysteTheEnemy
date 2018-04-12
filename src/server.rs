#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate tokio_core;
extern crate base64;
extern crate rand;

mod net;

use net::{RequestAction, ResponseCode, Packet, LineCodec};
/*
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
*/
use std::error::Error;
use std::io::{self, ErrorKind};
use std::iter;
use std::net::SocketAddr;
use std::process::exit;
use std::time::Duration;
use std::collections::HashMap;
use std::fmt;
use futures::*;
use futures::future::ok;
use futures::sync::mpsc;
use tokio_core::reactor::{Core, Timeout};
use rand::Rng;

const TICK_INTERVAL:      u64   = 40; // milliseconds
const MAX_GAME_SLOT_NAME: usize = 16;

#[derive(PartialEq, Debug, Clone, Copy)]
struct PlayerID(usize);

impl fmt::Display for PlayerID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({})", self.0)
    }
}

#[derive(PartialEq, Debug, Clone)]
struct Player {
    player_id:     PlayerID,
    cookie:        String,
    addr:          SocketAddr,
    player_name:   String,
    request_ack:   Option<u64>,          // most recent request sequence number received
    next_resp_seq: u64,                  // next response sequence number
    game_info:     Option<PlayerInGameInfo>,   // none means in lobby
}

// info for a player as it relates to a game/gameslot
#[derive(PartialEq, Debug, Clone)]
struct PlayerInGameInfo {
    game_slot_id: String,   // XXX remove or make as non-sequential ID (UUID?)
    //XXX PlayerGenState ID within Universe
    //XXX update statuses
}

impl Player {
    /*
    fn new(name: String, addr: SocketAddr) -> Self {
        let id = calculate_hash(&PlayerID {name: name.clone(), addr: addr});
        Player {
            player_name: name,
            player_id: id,
            addr: addr,
            in_game: false,
        }
    }
    */
}

/*
impl Hash for PlayerID {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.addr.hash(state);
    }
}
*/

#[derive(Clone)]
struct GameSlot {
    game_slot_id: String,
    name:         String,
    player_ids:   Vec<PlayerID>,
    game_running: bool,
    universe:     u64,    // Temp until we integrate
    pending_messages: Vec<(PlayerID, String)>
}

struct ServerState {
    tick:           u64,
    ctr:            u64,
    players:        Vec<Player>,
    player_map:     HashMap<String, PlayerID>,      // map cookie to player ID
    game_slots:     Vec<GameSlot>,
    next_player_id: PlayerID,  // index into players
}

//////////////// Utilities ///////////////////////

/*
fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
*/

fn new_cookie() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::encode(&buf)
}

impl GameSlot {
    fn new(name: String, player_ids: Vec<PlayerID>) -> Self {
        GameSlot {
            game_slot_id: new_cookie(),   // TODO: better unique ID generation
            name,
            player_ids:   player_ids,
            game_running: false,
            universe:     0,
            pending_messages: vec![]
        }
    }
}

impl ServerState {
    // not used for connect
    fn process_request_action(&mut self, player_id: PlayerID, action: RequestAction) -> ResponseCode {
        match action {
            RequestAction::Disconnect      => unimplemented!(),
            RequestAction::KeepAlive       => unimplemented!(),
            RequestAction::ListPlayers     => {
                let mut players = vec![];
                for ref p in &self.players {
                    players.push(p.player_name.clone());
                }
                ResponseCode::PlayerList(players)
            },
            RequestAction::ChatMessage(msg)  => {
                let player_in_game = self.players.iter()
                    .find(|p| p.player_id == player_id && p.game_info != None);

                match player_in_game {
                    Some(player) => {
                        // User is in game, Server needs to broadcast this to GameSlot
                        let slot_id = player.clone().game_info.unwrap().game_slot_id;

                        let mut found_slot = false;
                        for gs in &mut self.game_slots {
                            if gs.game_slot_id == slot_id {
                                let ref mut deliver : Vec<(PlayerID,String)> = gs.pending_messages;
                                deliver.push((player_id, msg));
                                found_slot = true;
                                break;
                            }
                        }
                        match found_slot {
                            true => ResponseCode::OK,
                            false => ResponseCode::BadRequest(Some(format!("Player \"{}\" not in game", player_id))),
                        }
                    }
                    None => {
                        ResponseCode::BadRequest(Some(format!("Player \"{}\" not found", player_id)))
                    }
                }
            },
            RequestAction::ListGameSlots   => {
                let mut slots = vec![];
                for ref gs in &self.game_slots {
                    slots.push((gs.name.clone(), gs.player_ids.len() as u64, gs.game_running));
                }
                ResponseCode::GameSlotList(slots)
            }
            RequestAction::NewGameSlot(name)  => {
                // validate length
                if name.len() > MAX_GAME_SLOT_NAME {
                    return ResponseCode::BadRequest(Some(format!("game slot name too long; max {} characters",
                                                                 MAX_GAME_SLOT_NAME)));
                }
                // XXX check name uniqueness
                // create game slot
                let game_slot = GameSlot::new(name, vec![]);
                self.game_slots.push(game_slot);
                ResponseCode::OK
            }
            RequestAction::JoinGameSlot(slot_name) => {
                let player: &mut Player = self.players.get_mut(player_id.0).unwrap();
                for ref mut gs in &mut self.game_slots {
                    if gs.name == slot_name {
                        gs.player_ids.push(player.player_id);

                        player.game_info = Some(PlayerInGameInfo {
                            game_slot_id: gs.clone().game_slot_id
                        });
                        // TODO: send event to in-game state machine
                        return ResponseCode::OK;
                    }
                }
                return ResponseCode::BadRequest(Some(format!("no game slot named {:?}", slot_name)));
            }
            RequestAction::LeaveGameSlot   => unimplemented!(),
            RequestAction::Connect{..}     => panic!(),
            RequestAction::None            => panic!(),
        }
    }

    fn is_unique_name(&self, name: &str) -> bool {
        for ref player in self.players.iter() {
            if player.player_name == name {
                return false;
            }
        }
        true
    }

    fn get_player_id_by_cookie(&self, cookie: &str) -> Option<PlayerID> {
        match self.player_map.get(cookie) {
            Some(player_id) => Some(*player_id),
            None => None
        }
    }

    // always returs either Ok(Some(Packet::Response{...})), Ok(None), or error
    fn decode_packet(&mut self, addr: SocketAddr, packet: Packet) -> Result<Option<Packet>, Box<Error>> {
        match packet {
            pkt @ Packet::Response{..} | pkt @ Packet::Update{..} => {
                return Err(Box::new(io::Error::new(ErrorKind::InvalidData, "invalid packet - server-only")));
            }
            Packet::Request{sequence, response_ack, cookie, action} => {
                match action {
                    RequestAction::Connect{..} => (),
                    _ => {
                        if cookie == None {
                            return Err(Box::new(io::Error::new(ErrorKind::InvalidData, "no cookie")));
                        }
                    }
                }

                // handle connect (create user, and save cookie)
                if let RequestAction::Connect{name, client_version} = action {
                    if self.is_unique_name(&name) {
                        let mut player = self.new_player(name.clone(), addr.clone());
                        let cookie = player.cookie.clone();
                        let sequence = player.next_resp_seq;
                        player.next_resp_seq += 1;

                        // save player into players vec, and save player ID into hash map using cookie
                        self.player_map.insert(cookie.clone(), player.player_id);
                        self.players.push(player);

                        let response = Packet::Response{
                            sequence:    sequence,
                            request_ack: None,
                            code:        ResponseCode::LoggedIn(cookie),
                        };
                        return Ok(Some(response));
                    } else {
                        // not a unique name
                        let response = Packet::Response{
                            sequence:    0,
                            request_ack: None,
                            code:        ResponseCode::Unauthorized(Some("not a unique name".to_owned())),
                        };
                        return Ok(Some(response));
                    }
                } else {
                    // look up player by cookie
                    let cookie = match cookie {
                        Some(cookie) => cookie,
                        None => {
                            return Err(Box::new(io::Error::new(ErrorKind::InvalidData, "cookie required for non-connect actions")));
                        }
                    };
                    let player_id = match self.get_player_id_by_cookie(cookie.as_str()) {
                        Some(player_id) => player_id,
                        None => {
                            return Err(Box::new(io::Error::new(ErrorKind::PermissionDenied, "invalid cookie")));
                        }
                    };
                    match action {
                        RequestAction::Connect{..} => unreachable!(),
                        _ => {
                            let response_code = self.process_request_action(player_id, action);
                            let sequence = {
                                let player: &mut Player = self.players.get_mut(player_id.0).unwrap();
                                let sequence = player.next_resp_seq;
                                player.next_resp_seq += 1;
                                sequence
                            };
                            let response = Packet::Response{
                                sequence:    sequence,
                                request_ack: None,
                                code:        response_code,
                            };
                            Ok(Some(response))
                        }
                    }
                }
            }
            Packet::UpdateReply{..} => {
                unimplemented!();
            }
        }
        /*
        match action {
            RequestAction::Connect => {
                self.players.iter().for_each(|player| {
                    assert_eq!(true, player.addr != addr && player.player_name != player_name);
                });

                self.players.push(Player::new(player_name, addr));
            },
            RequestAction::Ack                 => {},
            RequestAction::Disconnect          => {},
            RequestAction::JoinGame            => {},
            RequestAction::ListPlayers         => {},
            RequestAction::ChatMessage(String) => {},
            RequestAction::None                => {},
        }
        */
    }

    fn has_pending_players(&self) -> bool {
        !self.players.is_empty() && self.players.len() % 2 == 0
    }

    fn initiate_player_session(&mut self) {
        //XXX
        if self.has_pending_players() {
            if let Some(mut a) = self.players.pop() {
                if let Some(mut b) = self.players.pop() {
                    let game_slot = GameSlot::new("some game slot".to_owned(), vec![a.player_id, b.player_id]);
                    a.game_info = Some(PlayerInGameInfo{ game_slot_id: game_slot.game_slot_id.clone() });
                    b.game_info = Some(PlayerInGameInfo{ game_slot_id: game_slot.game_slot_id.clone() });
                    self.game_slots.push(game_slot);
                    self.ctr+=1;
                }
                else {
                    panic!("Unavailable player B");
                }
            }
            else {
                panic!("Unavailable player A");
            }
        }
    }

    fn new_player(&mut self, name: String, addr: SocketAddr) -> Player {
        let id = self.next_player_id;
        self.next_player_id = PlayerID(id.0 + 1);
        let cookie = new_cookie();
        Player {
            player_id:     id,
            cookie:        cookie,
            addr:          addr,
            player_name:   name,
            request_ack:   None,
            next_resp_seq: 0,
            game_info:     None,
        }
    }

    fn new() -> Self {
        ServerState {
            tick:              0,
            ctr:               0,
            players:           Vec::<Player>::new(),
            game_slots:        Vec::<GameSlot>::new(),
            player_map:        HashMap::<String, PlayerID>::new(),
            next_player_id:    PlayerID(0),
        }
    }
}

//////////////// Event Handling /////////////////
enum Event {
    TickEvent,
    Request((SocketAddr, Option<Packet>)),
//    Notify((SocketAddr, Option<Packet>)),
}

//////////////////// Main /////////////////////
fn main() {
    drop(env_logger::init());

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let (tx, rx) = mpsc::unbounded();

    let udp = net::bind(&handle, None, None)
        .unwrap_or_else(|e| {
            error!("Error while trying to bind UDP socket: {:?}", e);
            exit(1);
        });

    let (udp_sink, udp_stream) = udp.framed(LineCodec).split();

    let initial_server_state = ServerState::new();

    let iter_stream = stream::iter_ok::<_, io::Error>(iter::repeat( () ));
    let tick_stream = iter_stream.and_then(|_| {
        let timeout = Timeout::new(Duration::from_millis(TICK_INTERVAL), &handle).unwrap();
        timeout.and_then(move |_| {
            ok(Event::TickEvent)
        })
    }).map_err(|_| ());

    let packet_stream = udp_stream
        .filter(|&(_, ref opt_packet)| {
            *opt_packet != None
        })
        .map(|packet_tuple| {
            Event::Request(packet_tuple)
        })
        .map_err(|_| ());

    let server_fut = tick_stream
        .select(packet_stream)
        .fold((tx.clone(), initial_server_state), move |(tx, mut server_state), event| {
            match event {
                Event::Request(packet_tuple) => {
                     // With the above filter, `packet` should never be None
                    let (addr, opt_packet) = packet_tuple;
                    println!("got {:?} and {:?}!", addr, opt_packet);

                    if let Some(packet) = opt_packet {
                        let decode_result = server_state.decode_packet(addr, packet.clone());
                        if decode_result.is_ok() {
                            let opt_response_packet = decode_result.unwrap();
                            //XXX send packet
                            if let Some(response_packet) = opt_response_packet {
                                let response = (addr.clone(), response_packet);
                                tx.unbounded_send(response).unwrap();
                            }
                        } else {
                            let err = decode_result.unwrap_err();
                            println!("ERROR decoding packet from {:?}: {}", addr, err.description());
                        }
                    }
                }

                Event::TickEvent => {
                    // Server tick
                    // Likely spawn off work to handle server tasks here
                    server_state.tick += 1;

                    /*
                    server_state.initiate_player_session();

                    if server_state.ctr == 1 {
                        // GameSlot tick
                        server_state.game_slots.iter()
                            .filter(|ref conn| conn.player_a.in_game && conn.player_b.in_game)
                            .for_each(|ref conn| {
                                let player_a = &conn.player_a;
                                let player_b = &conn.player_b;
                                let uni = &conn.universe;
                                println!("Session: {}({:x}) versus {}({:x}), generation: {}",
                                    player_a.player_name, player_a.player_id,
                                    player_b.player_name, player_b.player_id,
                                    uni);
                            });

                        server_state.ctr += 1;
                    }
                    */
                }
            }

            // return the updated client for the next iteration
            ok((tx, server_state))
        })
        .map(|_| ())
        .map_err(|_| ());

    let sink_fut = rx.fold(udp_sink, |udp_sink, outgoing_item| {
            let udp_sink = udp_sink.send(outgoing_item).map_err(|_| ());    // this method flushes (if too slow, use send_all)
            udp_sink
        }).map(|_| ()).map_err(|_| ());

    let combined_fut = server_fut.map(|_| ())
        .select(sink_fut)
        .map(|_| ());   // wait for either server_fut or sink_fut to complete

    drop(core.run(combined_fut));
}

