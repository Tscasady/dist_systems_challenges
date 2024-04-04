use anyhow::Context;
use dist_systems_challenge::{service, InitPayload, Message, MsgBody, Node, NodeBuilder, Payload};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

struct EchoBuilder;

impl NodeBuilder<EchoNode> for EchoBuilder {
    fn build(self, msg: Message<InitPayload>, output: &mut StdoutLock) -> anyhow::Result<EchoNode> {
        let reply = Message {
            src: msg.dest,
            dest: msg.src,
            body: MsgBody {
                msg_id: Some(0),
                in_reply_to: msg.body.msg_id,
                payload: InitPayload::InitOk,
            },
        };
        serde_json::to_writer(&mut *output, &reply).context("serialize response to echo msg")?;
        output.write_all(b"\n").context("add newline")?;
        Ok(EchoNode { id: 1 })
    }
}

struct EchoNode {
    id: usize,
}

impl Node for EchoNode {
    type Payload = EchoPayload;
    fn reply(
        &mut self,
        input: Message<EchoPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            EchoPayload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.id += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

impl Payload for EchoPayload {}

fn main() -> anyhow::Result<()> {
    service(EchoBuilder {})
}

