use tracing::{debug, warn};

/// Calculate the QPS value of exploratory from the percentage
/// setting and QPS budget limit
pub fn qps_budget_exploratory(explore_budget_percent: u32, budget_limit_qps: u32) -> u32 {
    // Convert 5 -> 0.05
    let explore_ratio = explore_budget_percent as f32 / 100.0;

    (budget_limit_qps as f32 * explore_ratio) as u32
}

/// Calculate the free QPS share eligible for boost defined as
/// qps budget - passing qps - exploratory qps
pub fn qps_budget_boost(passing_qps: u32, budget_qps: u32, avail_qps: u32) -> u32 {
    budget_qps - passing_qps - qps_budget_exploratory(passing_qps, avail_qps)
}

/// Calculate the max budget of passing QPS defined as
/// qps limit - exploratory qps
pub fn qps_budget_passing(explore_qps_percent: u32, budet_limit_qps: u32) -> u32 {
    let explore_qps = qps_budget_exploratory(explore_qps_percent, budet_limit_qps);

    budet_limit_qps - explore_qps
}

/// Evaluate if this request passes exploratory QPS share,
/// See ['qps_exploratory()']
///
/// does this need to consider or offset for passing qps or anything else
/// depending on how often we call it? how do we ensure its the
/// proper percentage through and not over or under
pub fn qps_exploratory_passes(
    explore_budget_percent: u32,
    budget_limit_qps: u32,
    avail_qps: u32,
) -> bool {
    if avail_qps == 0 {
        warn!("avail_qps cannot be 0 when calculating exploratory QPS!");
        return false;
    }

    let explore_qps_budget = qps_budget_exploratory(explore_budget_percent, budget_limit_qps) as f32;

    // What percentage of available QPS can we use for exploration?
    let pass_probability = (explore_qps_budget / avail_qps as f32).min(1.0);

    debug!(
        "qps_exploratory_passes available {} limit {} percent {} rand threshold {}",
        avail_qps, budget_limit_qps, explore_budget_percent, pass_probability
    );

    rand::random_bool(pass_probability as f64)
}

/// Evaluate if a request passes the boost qps share
/// See ['qps_boost()']
pub fn qps_boost_passes(passing_qps: u32, budget_qps: u32, avail_qps: u32) -> bool {
    let boost_qps = budget_qps - passing_qps - qps_budget_exploratory(passing_qps, avail_qps);
    let boost_probability = boost_qps as f32 / avail_qps as f32;

    rand::random_bool(boost_probability as f64)
}
