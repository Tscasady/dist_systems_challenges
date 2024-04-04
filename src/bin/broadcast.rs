use anyhow::Context;
use dist_systems_challenge::*;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Serialize, Deserialize)]
struct BroadcastNode {
    id: String,
    msg_id: usize,
    read: Vec<usize>,
}

struct BroadcastNodeBuilder;

impl NodeBuilder<BroadcastNode> for BroadcastNodeBuilder {
    fn build(
        self,
        msg: Message<InitPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<BroadcastNode> {
        if let InitPayload::Init { node_id, .. } = msg.body.payload {
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

            return Ok(BroadcastNode {
                id: node_id,
                msg_id: 0,
                read: vec![],
            });
        } else {
            panic!()
        }
    }
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;

    fn reply(
        &mut self,
        input: dist_systems_challenge::Message<BroadcastPayload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Self::Payload::Broadcast { message } => {
                self.read.push(message);
                let reply = Message {
                    src: input.dest,
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
            }
            Self::Payload::Read => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Self::Payload::ReadOk {
                            messages: self.read.clone(),
                        },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to read msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.msg_id += 1;
            }
            Self::Payload::Topology => {
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
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum BroadcastPayload {
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<usize> },
    Topology,
    TopologyOk,
    Error,
}

impl Payload for BroadcastPayload {}

fn main() -> anyhow::Result<()> {
    service(BroadcastNodeBuilder {})
}
