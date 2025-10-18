use crate::app::context::StartupContext;
use crate::app::pipeline::ortb::build_rtb_pipeline;
use anyhow::Error;
use pipeline::BlockingTask;

pub struct BuildRtbPipelineTask;

impl BlockingTask<StartupContext, anyhow::Error> for BuildRtbPipelineTask {
    fn run(&self, context: &StartupContext) -> Result<(), Error> {
        build_rtb_pipeline(context)
    }
}