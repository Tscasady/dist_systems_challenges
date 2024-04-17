use dist_systems_challenge::*;
use serde::{Deserialize, Serialize};
use std::{io::StdoutLock, sync::mpsc::Sender};

#[derive(Serialize, Deserialize)]
struct UniqueNode {
    id: String,
    msg_id: usize,
}

impl Node for UniqueNode {
    type Payload = UniquePayload;

    fn new(mut msg: Message<InitPayload>, output: &mut StdoutLock, _tx: Sender<Message<UniquePayload>>) -> anyhow::Result<UniqueNode>
    {
        if let InitPayload::Init { node_id, .. } = std::mem::replace(&mut msg.body.payload, InitPayload::Empty) {
            msg.into_reply(Some(0), InitPayload::InitOk).write(&mut *output)?;

            return Ok(UniqueNode {
                id: node_id,
                msg_id: 1,
            })
        } else {
            panic!()
        }
    }

    fn reply(
        &mut self,
        input: Message<Self::Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Self::Payload::Generate => {
                let id = self.id.clone() + &self.msg_id.to_string();
                input.into_reply(Some(self.msg_id), Self::Payload::GenerateOk { id }).write(&mut *output)?;
                self.msg_id += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UniquePayload {
    Generate,
    GenerateOk { id: String }
}

impl Payload for UniquePayload {}

fn main() -> anyhow::Result<()> {
    service::<UniqueNode>()
}
