use crate::app::pipeline::ortb::AuctionContext;
use crate::app::pipeline::ortb::context::IdentityContext;
use crate::core::usersync;
use anyhow::{Error, anyhow};
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use pipeline::BlockingTask;
use rtb::bid_request::{DistributionchannelOneof, UserBuilder};
use rtb::child_span_info;
use std::sync::LazyLock;
use tracing::{Span, debug};

static COUNTER_BUYERUID_MATCHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("rex:supply:syncing")
        .u64_counter("syncing.supply_buyeruid_matches")
        .with_description("Count of supply specific buyeruid matches")
        .with_unit("1")
        .build()
});

/// Extract the existing buyeruid value from the incoming request,
/// which is common when an upstream supplier is hosting the match
/// table for us. This is likely from external programmatic partners
///
/// A Some(localuid) indicates a *validated* local id was present
fn extract_req_buyeruid(req: &rtb::BidRequest) -> Option<String> {
    match req.user.as_ref() {
        Some(user) => {
            if !user.buyeruid.is_empty() && usersync::utils::validate_local_id(&user.buyeruid) {
                debug!(
                    "Received buyeruid from supply request, skipping cookies header check: {}",
                    user.buyeruid
                );

                return Some(user.buyeruid.clone());
            }

            None
        }
        None => None,
    }
}

/// Extracts local uid value from cookies if present. This is likely
/// for direct publishers such as prebid and the like
fn extract_cookies_buyeruid(context: &AuctionContext, cookie_param: &str) -> Option<String> {
    let cookies = match &context.cookies {
        Some(cookies) => cookies,
        None => return None,
    };

    cookies.get(cookie_param).cloned()
}

/// Task responsible for extracting our local exchange uid value
/// and (in the future) any other identity values that apply
/// to this request as a whole e.g. RampId envelopes.
/// This extracts and attached them to the auction context,
/// and will place our primary local uid value in the
/// 'user.buyerid' value. A later task should handle
/// mapping and injecting of bidder specific values based
/// on our request-wide IDs this task handles
/// TODO this only handles web cookies and supply
/// provided buyeruid values for now, no external IDs yet
pub struct LocalIdentityTask;

impl LocalIdentityTask {
    fn run0(&self, context: &AuctionContext) -> Result<(), Error> {
        let span = Span::current();

        let mut req = context.req.write();

        if matches!(
            req.distributionchannel_oneof,
            Some(DistributionchannelOneof::App(_))
        ) {
            debug!("User syncing doesnt apply to app channel, skipping");

            if let Some(user) = req.user.as_mut() {
                if !&user.buyeruid.is_empty() {
                    debug!("What the heck, buyeruid value on app req? Clearing!");
                    user.buyeruid.clear();
                }
            }

            return Ok(());
        }

        let publisher = context
            .publisher
            .get()
            .ok_or_else(|| anyhow!("No publisher found in context!"))?;

        let mut attrs = vec![
            KeyValue::new("pub_id", publisher.id.clone()),
            KeyValue::new("pub_name", publisher.name.clone()),
        ];

        let local_uid = match extract_req_buyeruid(&req) {
            Some(uid) => {
                debug!("Received matched buyeruid in request");

                span.record("local_source", "request");
                attrs.push(KeyValue::new("local_source", "request"));

                uid
            }
            None => match extract_cookies_buyeruid(
                context,
                usersync::constants::CONST_REX_COOKIE_ID_PARAM,
            ) {
                Some(uid) => {
                    debug!("No buyeruid on request, but found in cookies");

                    span.record("local_source", "cookie");
                    attrs.push(KeyValue::new("local_source", "cookie"));

                    uid
                }
                None => {
                    debug!("No recognized user id found in request or cookies :(");

                    attrs.push(KeyValue::new("matched_user", false));
                    COUNTER_BUYERUID_MATCHES.add(1, &attrs);

                    return Ok(());
                }
            },
        };

        attrs.push(KeyValue::new("matched_user", true));
        COUNTER_BUYERUID_MATCHES.add(1, &attrs);

        span.record("local_uid", &local_uid);

        match req.user.as_mut() {
            Some(user) => {
                user.buyeruid.clear();

                user.id = local_uid.clone()
            }
            None => {
                let user = UserBuilder::default().id(local_uid.clone()).build()?;

                req.user = Some(user);
            }
        }

        let identity = context.identity.get_or_init(|| IdentityContext::default());

        identity
            .local_uid
            .set(local_uid.clone())
            .map_err(|_| anyhow!("Failed to set local_uid on identity context?!"))?;

        debug!("Recognized web user (local_uid) {}", local_uid);

        Ok(())
    }
}

impl BlockingTask<AuctionContext, Error> for LocalIdentityTask {
    fn run(&self, context: &AuctionContext) -> Result<(), Error> {
        let _span = child_span_info!(
            "user_syncs_task",
            local_uid = tracing::field::Empty,
            local_source = tracing::field::Empty
        );

        self.run0(context)
    }
}
