use dist_systems_challenge::{ service, Message, MsgBody, Node, Payload};
use std::io::{ StdoutLock, Write};
use anyhow::Context;

struct EchoNode {
    id: usize
}

impl Node for EchoNode {
    fn reply(
        &mut self,
        input: Message,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init {..} => {
                let reply = Message {
                    src: input.dest,
   dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to init msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.id += 1;
            }
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: MsgBody {
                        msg_id: Some(self.id),
                        in_reply_to: input.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    },
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response to echo msg")?;
                output.write_all(b"\n").context("add newline")?;
                self.id += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    service(EchoNode{ id: 0})
}
