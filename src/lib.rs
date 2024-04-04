use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{stdin, stdout, StdoutLock};

#[derive(Serialize, Deserialize)]
pub struct Message<Payload> {
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

pub trait Payload {}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

impl Payload for InitPayload {}

pub trait NodeBuilder<N: Node> {
    fn build(self, msg: Message<InitPayload>, output: &mut StdoutLock) -> anyhow::Result<N>;
}

pub trait Node {
    type Payload: Payload;
    fn reply(&mut self, input: Message<Self::Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn service<NB, N>(node_builder: NB) -> anyhow::Result<()>
where
    NB: NodeBuilder<N>,
    N: Node,
    <N as Node>::Payload: DeserializeOwned
{
    let mut stdin = stdin().lines();
    let mut stdout = stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("first message should be init")
            .context("failed to read init from stdin")?,
    )
    .context("init couldn't be deserde'd")?;

    let mut node = node_builder.build(init_msg, &mut stdout)?;

    for line in stdin {
        let line = line.context("input from stdin couldn't be read")?;
        let input: Message<N::Payload> = serde_json::from_str(&line).context("couldn't deserialize message")?;
        node.reply(input, &mut stdout)?
    }

    Ok(())
}

