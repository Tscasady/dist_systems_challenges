use core::fmt::Debug;
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{stdin, stdout, StdoutLock, Write};
use std::sync::mpsc::{channel, Sender};
use std::thread;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: MsgBody<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(&self, id: Option<usize>, payload: Payload) -> MessageBuilder<Payload> {
        MessageBuilder::empty()
            .src(self.dest.clone())
            .send_to(self.src.clone())
            .with_body(id, self.body.msg_id, payload)
    }

    // fn new() -> Self {
    //     todo!()
    // }

    pub fn write(self, output: &mut StdoutLock) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, &self).context("serialize message")?;
        output.write_all(b"\n").context("add newline")?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct MessageBuilder<Payload> {
    pub src: Option<String>,
    pub dest: Option<String>,
    pub body: Option<MsgBody<Payload>>,
}

impl<Payload> MessageBuilder<Payload> {
    pub fn empty() -> Self {
        MessageBuilder {
            src: None,
            dest: None,
            body: None,
        }
    }

    pub fn send_to(mut self, src: String) -> Self {
        self.dest = Some(src);
        self
    }

    pub fn src(mut self, dest: String) -> Self {
        self.src = Some(dest);
        self
    }

    pub fn with_body(
        mut self,
        msg_id: Option<usize>,
        in_reply_to: Option<usize>,
        payload: Payload,
    ) -> Self {
        self.body = Some(MsgBody {
            msg_id,
            in_reply_to,
            payload,
        });
        self
    }

    pub fn write(self, output: &mut StdoutLock) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, &self).context("serialize message")?;
        output.write_all(b"\n").context("add newline")?;
        Ok(())
    }

    pub fn build(self) -> Message<Payload> {
        Message {
            src: self.src.unwrap(),
            dest: self.dest.unwrap(),
            body: self.body.unwrap()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MsgBody<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

pub trait Payload: Debug {}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Empty
}

impl Payload for InitPayload {}

pub trait Node: Sized {
    type Payload: Payload;
    fn new(
        msg: Message<InitPayload>,
        output: &mut StdoutLock,
        tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<Self>;

    fn reply(
        &mut self,
        input: Message<Self::Payload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn service<N>() -> anyhow::Result<()>
where
    N: Node,
    N::Payload: DeserializeOwned + Sync + Send + 'static,
{
    let mut stdin = stdin().lines();
    let mut stdout = stdout().lock();

    let (sender, receiver) = channel::<Message<N::Payload>>();
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("first message should be init")
            .context("failed to read init from stdin")?,
    )
    .context("init couldn't be deserde'd")?;

    let mut node = N::new(init_msg, &mut stdout, sender.clone())?;

    let stdin_sender = sender.clone();
    drop(stdin);
    let handle = thread::spawn(move || {
        let stdin = std::io::stdin().lines();
        for line in stdin {
            let line = line.context("input from stdin couldn't be read")?;
            let input: Message<N::Payload> =
                serde_json::from_str(&line).context("couldn't deserialize message")?;
            if let Err(_) = stdin_sender.send(input) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        Ok(())
    });

    for input in receiver {
        node.reply(input, &mut stdout).context("couldn't reply")?
    }

    handle
        .join()
        .expect("stdin thread panicked")
        .context("stdin thread err'd")?;

    Ok(())
}
