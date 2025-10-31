use rtb::spec_list;

spec_list! {
    /// No buyers matched prefiltering
    NO_BUYERS_PREMATCHED = 500 => "No Buyers Prematched",

    /// No campaigns found and no bids received
    NO_CAMPAIGNS_FOUND = 501 => "No Campaigns Found",
    /// The publisher id is unrecognized
    UNKNOWN_SELLER = 502 => "Unknown Seller",
    SELLER_DISABLED = 503 => "Disabled Seller",
    THROTTLED_BUYER_QPS = 504 => "Throttled All Buyers For QPS",
}
