use anyhow::Error;
use async_trait::async_trait;
use pipeline::AsyncTask;
use crate::app::lifecycle::context::StartupContext;

pub(crate) struct StopServerTask;

#[async_trait]
impl AsyncTask<StartupContext, anyhow::Error> for StopServerTask {
    async fn run(&self, context: &StartupContext) -> Result<(), Error> {
        if context.server.get().is_none() {
            return Ok(())
        }
        
        match context.server.get() {
            Some(server) => {
                println!("Closing listener..");
                server.stop().await;
                println!("Listener closed.");
            },
            None => {
                println!("Skipping listener shutdown, was never started");
            }
        }

        Ok(())
    }
}