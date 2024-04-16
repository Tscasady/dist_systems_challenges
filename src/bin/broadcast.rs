use ::std::thread;
use anyhow::Context;
use dist_systems_challenge::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{StdoutLock, Write};
use std::sync::mpsc::Sender;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
    cluster: Vec<String>,
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;

    fn new(
        msg: Message<InitPayload>,
        output: &mut StdoutLock,
        gossip_trigger: Sender<Message<BroadcastPayload>>,
    ) -> anyhow::Result<BroadcastNode> {
        let node = if let InitPayload::Init { node_id, .. } = msg.body.payload {
            let reply = Message {
                src: msg.dest,
                dest: msg.src,
                body: MsgBody {
                    msg_id: Some(0),
                    in_reply_to: msg.body.msg_id,
                    payload: InitPayload::InitOk,
                },
            };
            serde_json::to_writer(&mut *output, &reply)
                .context("serialize response to broadcast init")?;
            output.write_all(b"\n").context("add newline")?;

            BroadcastNode {
                id: node_id,
                msg_id: 0,
                messages: HashSet::new(),
                cluster: vec![],
            }
        } else {
            panic!()
        };

        let gossip_timer = 500;

        thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(gossip_timer));

            if let Err(_) = gossip_trigger.send(Message {
                src: "".to_string(),
                dest: "".to_string(),
                body: MsgBody {
                    msg_id: None,
                    in_reply_to: None,
                    payload: BroadcastPayload::TriggerGossip,
                },
            }) {
                break;
            }
        });

        return Ok(node);
    }

    fn reply(
        &mut self,
        input: dist_systems_challenge::Message<BroadcastPayload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Self::Payload::Broadcast { message } => {
                self.messages.insert(message);
                let reply = Message {
                    src: input.dest.clone(),
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Self::Payload::BroadcastOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to broadcast msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.msg_id += 1;

                for node in &self.cluster {
                    let gossip = Message {
                        src: input.dest.clone(),
                        dest: node.to_string(),
                        body: MsgBody {
                            msg_id: Some(self.msg_id),
                            in_reply_to: input.body.msg_id,
                            payload: Self::Payload::Gossip { message },
                        },
                    };
                    serde_json::to_writer(&mut *output, &gossip).context("serialize gossip msg")?;
                    output.write_all(b"\n").context("add newline")?;
                    self.msg_id += 1;
                }
            }
            Self::Payload::Read => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Self::Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to read msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.msg_id += 1;
            }
            Self::Payload::Topology { topology } => {
                self.cluster = topology.get(&self.id).unwrap().clone();
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Self::Payload::TopologyOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to topology msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.msg_id += 1;
            }
            Self::Payload::Gossip { message } => {
                if self.messages.insert(message) {
                    for node in &self.cluster {
                        let gossip = Message {
                            src: input.dest.clone(),
                            dest: node.to_string(),
                            body: MsgBody {
                                msg_id: Some(self.msg_id),
                                in_reply_to: input.body.msg_id,
                                payload: Self::Payload::Gossip { message },
                            },
                        };
                        serde_json::to_writer(&mut *output, &gossip)
                            .context("serialize gossip msg")?;
                        output.write_all(b"\n").context("add newline")?;
                        self.msg_id += 1;
                    }
                };
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum BroadcastPayload {
    TriggerGossip,
    Gossip {
        message: usize,
    },
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

impl Payload for BroadcastPayload {}

fn main() -> anyhow::Result<()> {
    service::<BroadcastNode>()
}
