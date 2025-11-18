/// Apply publisher margin to the gross bid price received from the
/// demand partner.
///
/// # Example
/// If bid price if $10 and take rate is 10%,
/// the resulting bid price is 10 * 0.10 -> $9.0
pub fn markdown_bid(original_bid_price: f64, take_rate: u32) -> f64 {
    if take_rate == 0 {
        return original_bid_price;
    }

    let margin_factor = take_rate as f64 / 100.0;

    original_bid_price - original_bid_price * margin_factor
}

/// Markup the floor price of a request to ensure
/// that if a bid is received exactly at the floor
/// it is sufficient to meet or exceed the original
/// floor post bid ['markdown_bid()'] adjustment
///
/// Formula: markup_floor = original_floor / (1 - take_rate_percentage/100)
///
/// # Example
/// If the original floor is $10.0 and take_rate is 10%:
/// - Markup floor: $10.0 / (1 - 10/100) = $10.0 / 0.90 = $11.11
/// - If bid comes in at $11.11, markdown: $11.11 * 0.10 = $1.11
/// - Publisher receives: $11.11 - $1.11 = $10.00
pub fn markup_floor(original_bid_floor: f64, take_rate: u32) -> f64 {
    if take_rate == 0 {
        return original_bid_floor;
    }

    let margin_factor = take_rate as f64 / 100.0;

    original_bid_floor / (1.0 - margin_factor)
}
