use crate::core::events::DataUrl;
use anyhow::Error;
use derive_builder::Builder;
use rtb::utils::adformats::AdFormat;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::{Display, EnumString};

/// Url param field key for the timestamp the bid was made
pub const FIELD_BID_TIMESTAMP: &str = "ts";
/// Url param field key for the locally assigned but globaly unique auction id
pub const FIELD_AUCTION_EVENT_ID: &str = "aei";
/// Url param field key for the unique locally ssigned bid event id
pub const FIELD_BID_EVENT_ID: &str = "bei";
/// Url param key for the gross cpm price (charged to bidder)
pub const FIELD_CPM_GROSS: &str = "cg";
/// Url param key for the pub cpm cost (what we pay the pub)
pub const FIELD_CPM_COST: &str = "cc";
/// Url param key for bidder id
pub const FIELD_BIDDER_ID: &str = "bi";
/// Url param key for bidder endpoint id
pub const FIELD_ENDPOINT_ID: &str = "ei";
/// Url param key for publisher id
pub const FIELD_PUB_ID: &str = "pi";
/// Url param key for the bid ad format
pub const FIELD_BID_AD_FORMAT: &str = "f";
/// The location and invocation source of this event,
/// see ['EventSource']
///
pub const FIELD_EVENT_SOURCE: &str = "s";

/// Source of billing event
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum EventSource {
    /// The bid.burl field fired s2s from publisher
    Burl,
    /// Placed as an ad markup beacon, either as html 1x1 pixel,
    /// vast <Impression> entry, or native imptracker
    Adm,
}

/// Primary fields used to produce or extract details from a billing event url
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Builder)]
pub struct BillingEvent {
    pub bid_timestamp: u64,
    pub auction_event_id: String,
    pub bid_event_id: String,
    pub cpm_gross: f32,
    pub cpm_cost: f32,
    pub bidder_id: String,
    pub endpoint_id: String,
    pub pub_id: String,
    pub bid_ad_format: AdFormat,
    pub event_source: EventSource,
}

impl BillingEvent {
    /// Extracts a well structured ['BillingEvent'] from a ['DataUrl']
    pub fn from(data_url: &DataUrl) -> Result<Self, Error> {
        let ad_format_str = data_url.get_required_string(FIELD_BID_AD_FORMAT)?;
        let bid_ad_format = AdFormat::from_str(&ad_format_str)?;

        let event_source_str = data_url.get_required_string(FIELD_EVENT_SOURCE)?;
        let event_source = EventSource::from_str(&event_source_str)?;

        Ok(BillingEventBuilder::default()
            .bid_timestamp(data_url.get_required_int(FIELD_BID_TIMESTAMP)? as u64)
            .auction_event_id(data_url.get_required_string(FIELD_AUCTION_EVENT_ID)?)
            .bid_event_id(data_url.get_required_string(FIELD_BID_EVENT_ID)?)
            .cpm_gross(data_url.get_required_float(FIELD_CPM_GROSS)? as f32)
            .cpm_cost(data_url.get_required_float(FIELD_CPM_COST)? as f32)
            .bidder_id(data_url.get_required_string(FIELD_BIDDER_ID)?)
            .endpoint_id(data_url.get_required_string(FIELD_ENDPOINT_ID)?)
            .pub_id(data_url.get_required_string(FIELD_PUB_ID)?)
            .bid_ad_format(bid_ad_format)
            .event_source(event_source)
            .build()?)
    }

    /// Writes the billing event field/value pairs to a ['DataUrl']
    pub fn write_to(&self, data_url: &mut DataUrl) -> Result<(), Error> {
        data_url
            .add_int(FIELD_BID_TIMESTAMP, self.bid_timestamp as i64)?
            .add_string(FIELD_AUCTION_EVENT_ID, &self.auction_event_id)?
            .add_string(FIELD_BID_EVENT_ID, &self.bid_event_id)?
            .add_float(FIELD_CPM_GROSS, self.cpm_gross as f64)?
            .add_float(FIELD_CPM_COST, self.cpm_cost as f64)?
            .add_string(FIELD_BIDDER_ID, &self.bidder_id)?
            .add_string(FIELD_ENDPOINT_ID, &self.endpoint_id)?
            .add_string(FIELD_PUB_ID, &self.pub_id)?
            .add_string(FIELD_BID_AD_FORMAT, &self.bid_ad_format.to_string())?
            .add_string(FIELD_EVENT_SOURCE, &self.event_source.to_string())?;
        Ok(())
    }
}
