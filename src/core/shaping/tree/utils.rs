/// Returns the effective available QPS value for
/// this observed endpoint on this local node,
/// defined as min(local qps limit, avail qps)
/// if local qps > 0, or state_avail_qps if no
/// endpoint qps limit
pub fn calc_effective_avail_pool(local_qps_limit: u32, state_avail_qps: u32) -> u32 {
    if local_qps_limit > 0 {
        local_qps_limit.min(state_avail_qps)
    } else {
        state_avail_qps
    }
}

/// Calculate the QPS value of exploratory from a percentage
/// of the available qps pool, which should be the min
/// of available qps or endpoint limit.
/// returns percentageof avail qps or 1 if 0 pool size
pub fn qps_budget_exploratory(explore_budget_percent: u32, avail_qps_pool: u32) -> u32 {
    if explore_budget_percent == 0 || avail_qps_pool == 0 {
        return 1;
    }

    // Convert 5 -> 0.05
    let explore_ratio = explore_budget_percent.max(1) as f32 / 100.0;

    (avail_qps_pool as f32 * explore_ratio) as u32
}

/// Calculate the max budget of passing QPS defined as
/// qps limit - exploratory qps
pub fn qps_budget_passing(explore_qps_percent: u32, avail_qps_pool: u32) -> u32 {
    let explore_qps = qps_budget_exploratory(explore_qps_percent, avail_qps_pool);

    if avail_qps_pool == 0 {
        return 0;
    }

    avail_qps_pool.saturating_sub(explore_qps)
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
