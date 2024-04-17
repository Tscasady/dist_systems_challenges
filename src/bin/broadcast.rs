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
    unconfirmed: HashMap<String, HashSet<usize>>,
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;

    fn new(
        mut msg: Message<InitPayload>,
        output: &mut StdoutLock,
        gossip_trigger: Sender<Message<BroadcastPayload>>,
    ) -> anyhow::Result<BroadcastNode> {
        let node = if let InitPayload::Init { node_id, .. } =
            std::mem::replace(&mut msg.body.payload, InitPayload::Empty)
        {
            msg.into_reply(Some(0), InitPayload::InitOk)
                .write(&mut *output)?;

            BroadcastNode {
                id: node_id,
                msg_id: 0,
                messages: HashSet::new(),
                cluster: vec![],
                unconfirmed: HashMap::new(),
            }
        } else {
            panic!()
        };

        let gossip_timer = 300;

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

                for node in &self.cluster {
                    input
                        .into_reply(Some(self.msg_id), Self::Payload::Gossip { message })
                        .send_to(node.clone())
                        .write(&mut *output)?;
                    self.msg_id += 1;
                    self.unconfirmed.entry(node.clone()).and_modify(|set| {
                        set.insert(message);
                    });
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
                self.unconfirmed
                    .extend(self.cluster.iter().map(|n| (n.clone(), HashSet::default())));
                input
                    .into_reply(Some(self.msg_id), Self::Payload::TopologyOk)
                    .write(&mut *output)?;
                self.msg_id += 1;
            }
            Self::Payload::Gossip { message } => {
                if self.messages.insert(message) {
                    for node in &self.cluster {
                        if node != &input.src {
                            input
                                .into_reply(Some(self.msg_id), Self::Payload::Gossip { message })
                                .send_to(node.to_string())
                                .write(&mut *output)?;
                            self.msg_id += 1;
                            self.unconfirmed.entry(node.clone()).and_modify(|set| {
                                set.insert(message);
                            });
                        }
                    }
                };

                input
                    .into_reply(Some(self.msg_id), BroadcastPayload::GossipOk { message })
                    .write(&mut *output)?;
                self.msg_id += 1;
            }
            Self::Payload::GossipOk { message } => {
                self.unconfirmed.entry(input.src).and_modify(|set| {
                    set.remove(&message);
                });
            }
            Self::Payload::TriggerGossip => {
                eprintln!("{:?}", &self.unconfirmed);
                for (node, messages) in &self.unconfirmed {
                    for message in messages {
                        MessageBuilder::empty()
                            .send_to(node.to_string())
                            .src(self.id.clone())
                            .with_body(
                                Some(self.msg_id),
                                None,
                                BroadcastPayload::Gossip { message: *message },
                            )
                            .write(&mut *output)?;
                        self.msg_id += 1;
                    }
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
        message: usize,
    },
    GossipOk {
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
