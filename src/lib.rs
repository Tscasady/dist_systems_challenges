use serde::{Deserialize, Serialize};
use std::io::{stdin, stdout, StdoutLock};
use anyhow::Context;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MsgBody<Payload>,
}

#[derive(Serialize, Deserialize)]
pub struct MsgBody<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Error,
}

pub struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

pub trait Node {
    fn reply(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()>;
}
pub fn service<N: Node>(mut node: N) -> anyhow::Result<()> {
    let stdin = stdin().lock();
    let mut stdout = stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    for input in inputs {
        let input = input.context("couldn't deserialize message")?;
        node.reply(input, &mut stdout)?
    }

    Ok(())
}
