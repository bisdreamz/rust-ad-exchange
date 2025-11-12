pub mod constants;
/// Module containing pipelines for processing all event types (burl, lurl, etc)
pub mod events;
/// Pipeline for processing bidrequests in and auction process. Could be prefixed with
/// other pipelines such as vast or prebid to extend functionality
pub mod ortb;
pub mod syncing;
