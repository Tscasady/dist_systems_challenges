use dist_systems_challenge::{service, InitPayload, Message, Node, Payload};
use serde::{Deserialize, Serialize};
use std::{
    io::StdoutLock,
    sync::mpsc::Sender,
};

struct EchoNode {
    id: usize,
}

impl Node for EchoNode {
    type Payload = EchoPayload;

    fn new(
        msg: Message<InitPayload>,
        output: &mut StdoutLock,
        _tx: Sender<Message<EchoPayload>>,
    ) -> anyhow::Result<EchoNode> {
        msg.into_reply(Some(0), InitPayload::InitOk).write(&mut *output)?;
        Ok(EchoNode { id: 1 })
    }

    fn reply(
        &mut self,
        input: Message<EchoPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            EchoPayload::Echo { ref echo } => {
                input.into_reply(Some(self.id), EchoPayload::EchoOk { echo: echo.clone() }).write(&mut *output)?;
                self.id += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
    Empty
}

impl Payload for EchoPayload {}

fn main() -> anyhow::Result<()> {
    service::<EchoNode>()
}
