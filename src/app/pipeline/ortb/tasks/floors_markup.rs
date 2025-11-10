use crate::app::pipeline::ortb::AuctionContext;
use anyhow::{bail, Error};
use async_trait::async_trait;
use pipeline::AsyncTask;
use rtb::child_span_info;
use tracing::{debug, Instrument};
use crate::core::demand::takerate;

pub const MIN_FLOOR: f64 = 0.25;

pub struct FloorsMarkupTask;

impl FloorsMarkupTask {
    async fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let publisher = match context.publisher.get() {
            Some(publisher) => publisher,
            None => bail!("publisher is present on ctx, cant markup floors!"),
        };

        let mut req = context.req.write();

        for imp in req.imp.iter_mut() {
            if imp.bidfloor < MIN_FLOOR {
                debug!("Applying min floor to imp &{} -> &{}", imp.bidfloor, MIN_FLOOR);
                imp.bidfloor = MIN_FLOOR;
                continue;
            }

            let new_imp_floor = takerate::markup_floor(imp.bidfloor, publisher.margin);
            debug!("Marking up imp floor from ${} -> ${}", imp.bidfloor, new_imp_floor);

            imp.bidfloor = new_imp_floor;

            let pmp = match &mut imp.pmp {
                Some(pmp) => pmp,
                None => continue,
            };

            for deal in pmp.deals.iter_mut() {
                // we need to markup deal floors, too
                // and ensure they meet or exceed the imp floor
                let min_deal_floor = takerate::markup_floor(deal.bidfloor, publisher.margin);
                let new_deal_floor = min_deal_floor.max(new_imp_floor);

                debug!("Bringing deal floor ${} up to new imp floor ${}",
                        deal.bidfloor, new_deal_floor);

                deal.bidfloor = new_deal_floor;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncTask<AuctionContext, Error> for FloorsMarkupTask {
    async fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = child_span_info!("floors_markup_task");

        self.run0(context).instrument(span).await
    }
}