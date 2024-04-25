use ::std::thread;
use dist_systems_challenge::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;
use std::sync::mpsc::Sender;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
    cluster: Vec<String>,
    confirmed: HashMap<String, HashSet<usize>>,
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;

    fn new(
        msg: Message<InitPayload>,
        output: &mut StdoutLock,
        gossip_trigger: Sender<Message<BroadcastPayload>>,
    ) -> anyhow::Result<BroadcastNode> {
        let node = if let InitPayload::Init { ref node_id, .. } = msg.body.payload {
            msg.into_reply(Some(0), InitPayload::InitOk)
                .write(&mut *output)?;

            BroadcastNode {
                id: node_id.clone(),
                msg_id: 0,
                messages: HashSet::new(),
                cluster: vec![],
                confirmed: HashMap::new(),
            }
        } else {
            panic!()
        };

        let gossip_timer = 1000;

        let node_id = node.id.clone();
        thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(gossip_timer));

            if let Err(_) = gossip_trigger.send(
                MessageBuilder::empty()
                    .send_to(node_id.clone())
                    .src(node_id.clone())
                    .with_body(None, None, BroadcastPayload::TriggerGossip)
                    .build(),
            ) {
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
                input
                    .into_reply(Some(self.msg_id), Self::Payload::BroadcastOk)
                    .write(&mut *output)?;
                self.msg_id += 1;

                //consider doing nothing on broadcast, only gossip on a timer
                for node in &self.cluster {
                    if !self
                        .confirmed
                        .get(node)
                        .map(|known| known.contains(&message))
                        .expect("node should be in cluster")
                    {
                        let mut messages = HashSet::new();
                        messages.insert(message);
                        input
                            .into_reply(Some(self.msg_id), Self::Payload::Gossip { messages })
                            .send_to(node.clone())
                            .write(&mut *output)?;
                        self.msg_id += 1;
                    }
                }
            }
            Self::Payload::Read => {
                input
                    .into_reply(
                        Some(self.msg_id),
                        Self::Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                    )
                    .write(&mut *output)?;
                self.msg_id += 1;
            }
            Self::Payload::Topology { ref topology } => {
                self.cluster = topology.get(&self.id).unwrap().clone();
                self.confirmed
                    .extend(self.cluster.iter().map(|n| (n.clone(), HashSet::default())));
                input
                    .into_reply(Some(self.msg_id), Self::Payload::TopologyOk)
                    .write(&mut *output)?;
                self.msg_id += 1;
            }
            Self::Payload::Gossip { ref messages } => {
                self.confirmed.entry(input.src.clone()).and_modify(|set| {
                    set.extend(messages.clone());
                });

                for message in messages.iter() {
                    if self.messages.insert(*message) {
                        for node in &self.cluster {
                            if node != &input.src {
                                let mut messages = HashSet::new();
                                messages.insert(*message);
                                input
                                    .into_reply(
                                        Some(self.msg_id),
                                        Self::Payload::Gossip { messages },
                                    )
                                    .send_to(node.to_string())
                                    .write(&mut *output)?;
                                self.msg_id += 1;
                            }
                        }
                    }
                }
            }
            Self::Payload::Share { known } => {
                for (node, message) in known {
                    self.confirmed
                        .entry(node)
                        .and_modify(|set| set.extend(message));
                }
            }
            Self::Payload::TriggerGossip => {
                for (node, known) in &self.confirmed {
                    MessageBuilder::empty()
                        .send_to(node.to_string())
                        .src(self.id.clone())
                        .with_body(
                            Some(self.msg_id),
                            None,
                            BroadcastPayload::Gossip {
                                messages: self.messages.difference(known).cloned().collect(),
                            },
                        )
                        .write(&mut *output)?;
                    self.msg_id += 1;

                    MessageBuilder::empty()
                        .send_to(node.to_string())
                        .src(self.id.clone())
                        .with_body(
                            Some(self.msg_id),
                            None,
                            BroadcastPayload::Share {
                                known: self.confirmed.clone(),
                            },
                        )
                        .write(&mut *output)?;
                    self.msg_id += 1;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum BroadcastPayload {
    TriggerGossip,
    Gossip {
        messages: HashSet<usize>,
    },
    Share {
        known: HashMap<String, HashSet<usize>>,
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
