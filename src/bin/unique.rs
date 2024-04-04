use anyhow::Context;
use dist_systems_challenge::*;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Serialize, Deserialize)]
struct UniqueNode {
    id: String,
    msg_id: usize,
}

struct UniqueNodeBuilder;

impl NodeBuilder<UniqueNode> for UniqueNodeBuilder {
    fn build(self, msg: Message<InitPayload>, output: &mut StdoutLock) -> anyhow::Result<UniqueNode>
    {
        if let InitPayload::Init { node_id, .. } = msg.body.payload {
            let reply = Message {
                src: msg.dest,
                dest: msg.src,
                body: MsgBody {
                    msg_id: Some(0),
                    in_reply_to: msg.body.msg_id,
                    payload: InitPayload::InitOk
                }
            };
            serde_json::to_writer(&mut *output, &reply).context("serialize response to echo msg")?;
            output.write_all(b"\n").context("add newline")?;

            return Ok(UniqueNode {
                id: node_id,
                msg_id: 1,
            })
        } else {
            panic!()
        }
    }
}

impl Node for UniqueNode {
    type Payload = UniquePayload;
    fn reply(
        &mut self,
        input: dist_systems_challenge::Message<Self::Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Self::Payload::Generate => {
                let id = self.id.clone() + &self.msg_id.to_string();
                let reply = Message {
                    src: self.id.clone(),
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Self::Payload::GenerateOk { id },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init msg")?;
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
enum UniquePayload {
    Generate,
    GenerateOk { id: String }
}

impl Payload for UniquePayload {}

fn main() -> anyhow::Result<()> {
    service(UniqueNodeBuilder {})
}
