use dist_systems_challenge::*;
use std::io::Write;
use serde::{Deserialize, Serialize};
use anyhow::Context;

#[derive(Serialize, Deserialize)]
struct UniqueNode {
    id: Option<String>,
    msg_id: usize
}

impl Node for UniqueNode {
    fn reply(
        &mut self,
        input: dist_systems_challenge::Message,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { node_id, .. } => {
                self.id = Some(node_id);
                let reply = Message {
                    src: self.id.clone().unwrap(),
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to init msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.msg_id += 1;
            }
            Payload::Generate => {
                let id = self.id.clone().unwrap() + &self.msg_id.to_string();
                let reply = Message {
                    src: self.id.clone().unwrap(),
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::GenerateOk { id },
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

fn main() -> anyhow::Result<()> {
    let node = UniqueNode { msg_id: 0, id: None };
    service(node)
}
