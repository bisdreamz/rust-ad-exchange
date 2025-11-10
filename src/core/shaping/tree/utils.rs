/// Calculate the QPS value of exploratory from a percentage.
/// This returns the effective QPS derived from exploratory percent
/// of the budget_limit_qps if > 0, otherwise as a percentage of avail
pub fn qps_budget_exploratory(
    explore_budget_percent: u32,
    budget_limit_qps: u32,
    avail_qps_pool: u32,
) -> u32 {
    // Convert 5 -> 0.05
    let explore_ratio = explore_budget_percent.max(1) as f32 / 100.0;

    let exploratory_qps = if budget_limit_qps == 0 {
        // no qps limit, calculate as percent of avail
        (avail_qps_pool as f32 * explore_ratio) as u32
    } else {
        // we have a qps limit
        (budget_limit_qps as f32 * explore_ratio) as u32
    };

    exploratory_qps
}

/// Calculate the free QPS share eligible for boost defined as
/// qps budget - passing qps - exploratory qps
pub fn qps_budget_boost(
    passing_qps: u32,
    exploratory_qps: u32,
    budget_limit_qps: u32,
    avail_qps_pool: u32,
) -> u32 {
    if budget_limit_qps == 0 {
        return avail_qps_pool
            .saturating_sub(passing_qps)
            .saturating_sub(exploratory_qps);
    }

    budget_limit_qps
        .saturating_sub(passing_qps)
        .saturating_sub(exploratory_qps)
}

/// Calculate the max budget of passing QPS defined as
/// qps limit - exploratory qps
pub fn qps_budget_passing(
    explore_qps_percent: u32,
    budget_limit_qps: u32,
    avail_qps_pool: u32,
) -> u32 {
    let explore_qps = qps_budget_exploratory(explore_qps_percent, budget_limit_qps, avail_qps_pool);

    if avail_qps_pool == 0 {
        return 0;
    }

    if budget_limit_qps > 0 {
        budget_limit_qps.saturating_sub(explore_qps)
    } else {
        avail_qps_pool.saturating_sub(explore_qps)
    }
}

/// Generic method to assess random probability of whether this request
/// passes or not, given the relation of target_qps to avail_qps_pool
/// E.g. 200 target / 1000 avail = 20% chance of passing
///
/// # Arguments
/// * 'target_qps' - The desired QPS
/// * 'avail_qps_pool' - The available QPS to pass a share of. It is
/// important that this is a true reflection of available QPS at
/// the point in which this function is called!
pub fn qps_passes_percentage(target_qps: u32, avail_qps_pool: u32) -> bool {
    if avail_qps_pool == 0 {
        return false;
    }

    if target_qps >= avail_qps_pool {
        return true;
    }

    let probability = target_qps as f32 / avail_qps_pool as f32;

    rand::random_bool(probability as f64)
}
