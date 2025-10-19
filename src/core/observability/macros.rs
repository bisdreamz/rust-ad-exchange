
/// Creates a root span based on the provided sampling rate.
/// This is required because if we simply used the instrument
/// attribute then it would still do a lot of the heavy lifting
/// in terms of collecting and cloning context, which is
/// convenient but bad for performance. This makes span sampling
/// a pre-filter, and while more effort it prevents the overhead.
///
/// # Arguments
/// * `sample_percent` - The percent (0.0 to 1.0) of spans to sample
/// * `span_name` - The name of the span if created (must be a literal)
///
/// # Behavior
/// - If a parent span exists (is active): ALWAYS creates a child span (preserves complete trace)
/// - If no parent exists: Makes sampling decision at the configured rate
///
/// This implements head-based sampling where the root makes the decision,
/// and all children are included to maintain trace completeness.
///
/// # Returns
/// - Real span if parent exists OR sampling passes
/// - `Span::none()` if no parent and sampling fails
///
/// # Example
/// ```
/// let span = sample_or_attach_root_span!(0.01, "rtb_pipeline");
/// let _guard = span.enter();
/// ```
#[macro_export]
macro_rules! sample_or_attach_root_span {
    ($sample_percent:expr, $span_name:literal) => {{
        let current = tracing::Span::current();

        if !current.is_disabled() || rand::random::<f32>() < $sample_percent {
            tracing::info_span!($span_name)
        } else {
            tracing::Span::none()
        }
    }};
}

/// Creates a TRACE-level child span only if the parent span is active (sampled).
///
/// This enables zero-overhead span creation for unsampled requests - when the
/// parent span is disabled, this returns `Span::none()` without any overhead.
///
/// # Returns
/// An **un-entered** `Span` - you must call `.entered()` or use `.instrument()`.
///
/// # Arguments
/// * `span_name` - Name for the span (must be a literal)
/// * `fields` - Optional span fields (e.g., `field1 = value1, field2 = %value2`)
///
/// # Examples
/// ```
/// // Without fields
/// let span = child_span_trace!("detail_task");
/// let _enter = span.entered();
///
/// // With fields
/// let span = child_span_trace!("process", item_id = %id, count = items.len());
/// do_work().instrument(span).await;  // For async
/// ```
///
/// # See also
/// - [`sample_or_attach_root_span!`] for creating root spans with sampling
#[macro_export]
macro_rules! child_span_trace {
    ($span_name:literal) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::trace_span!($span_name)
        } else {
            ::tracing::Span::none()
        }
    }};
    ($span_name:literal, $($fields:tt)*) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::trace_span!($span_name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    }};
}

/// Creates a DEBUG-level child span only if the parent span is active (sampled).
///
/// This enables zero-overhead span creation for unsampled requests - when the
/// parent span is disabled, this returns `Span::none()` without any overhead.
///
/// # Returns
/// An **un-entered** `Span` - you must call `.entered()` or use `.instrument()`.
///
/// # Arguments
/// * `span_name` - Name for the span (must be a literal)
/// * `fields` - Optional span fields (e.g., `field1 = value1, field2 = %value2`)
///
/// # Examples
/// ```
/// // Without fields
/// let span = child_span_debug!("subtask");
/// let _enter = span.entered();
///
/// // With fields
/// let span = child_span_debug!("validate", user_id = %user.id);
/// span.record("result", "success");
/// ```
///
/// # See also
/// - [`sample_or_attach_root_span!`] for creating root spans with sampling
#[macro_export]
macro_rules! child_span_debug {
    ($span_name:literal) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::debug_span!($span_name)
        } else {
            ::tracing::Span::none()
        }
    }};
    ($span_name:literal, $($fields:tt)*) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::debug_span!($span_name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    }};
}

/// Creates an INFO-level child span only if the parent span is active (sampled).
///
/// This enables zero-overhead span creation for unsampled requests - when the
/// parent span is disabled, this returns `Span::none()` without any overhead.
///
/// # Returns
/// An **un-entered** `Span` - you must call `.entered()` or use `.instrument()`.
///
/// # Arguments
/// * `span_name` - Name for the span (must be a literal)
/// * `fields` - Optional span fields (e.g., `field1 = value1, field2 = %value2`)
///
/// # Examples
/// ```
/// // Without fields
/// let span = child_span_info!("task_name");
/// let _enter = span.entered();
///
/// // With fields
/// let span = child_span_info!("bidder_match", bidder = %bidder.name, count = endpoints.len());
/// span.record("result", "matched");
/// ```
///
/// # See also
/// - [`sample_or_attach_root_span!`] for creating root spans with sampling
#[macro_export]
macro_rules! child_span_info {
    ($span_name:literal) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::info_span!($span_name)
        } else {
            ::tracing::Span::none()
        }
    }};
    ($span_name:literal, $($fields:tt)*) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::info_span!($span_name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    }};
}

/// Creates a WARN-level child span only if the parent span is active (sampled).
///
/// This enables zero-overhead span creation for unsampled requests - when the
/// parent span is disabled, this returns `Span::none()` without any overhead.
///
/// # Returns
/// An **un-entered** `Span` - you must call `.entered()` or use `.instrument()`.
///
/// # Arguments
/// * `span_name` - Name for the span (must be a literal)
/// * `fields` - Optional span fields (e.g., `field1 = value1, field2 = %value2`)
///
/// # Examples
/// ```
/// // Without fields
/// let span = child_span_warn!("slow_operation");
/// let _enter = span.entered();
///
/// // With fields
/// let span = child_span_warn!("timeout", duration_ms = elapsed, threshold = 1000);
/// span.record("action", "retrying");
/// ```
///
/// # See also
/// - [`sample_or_attach_root_span!`] for creating root spans with sampling
#[macro_export]
macro_rules! child_span_warn {
    ($span_name:literal) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::warn_span!($span_name)
        } else {
            ::tracing::Span::none()
        }
    }};
    ($span_name:literal, $($fields:tt)*) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::warn_span!($span_name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    }};
}

/// Creates an ERROR-level child span only if the parent span is active (sampled).
///
/// This enables zero-overhead span creation for unsampled requests - when the
/// parent span is disabled, this returns `Span::none()` without any overhead.
///
/// # Returns
/// An **un-entered** `Span` - you must call `.entered()` or use `.instrument()`.
///
/// # Arguments
/// * `span_name` - Name for the span (must be a literal)
/// * `fields` - Optional span fields (e.g., `field1 = value1, field2 = %value2`)
///
/// # Examples
/// ```
/// // Without fields
/// let span = child_span_error!("critical_failure");
/// let _enter = span.entered();
///
/// // With fields
/// let span = child_span_error!("database_error", error = %err, query = %sql);
/// span.record("recovery_attempted", true);
/// ```
///
/// # See also
/// - [`sample_or_attach_root_span!`] for creating root spans with sampling
#[macro_export]
macro_rules! child_span_error {
    ($span_name:literal) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::error_span!($span_name)
        } else {
            ::tracing::Span::none()
        }
    }};
    ($span_name:literal, $($fields:tt)*) => {{
        if !::tracing::Span::current().is_disabled() {
            ::tracing::error_span!($span_name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    }};
}

