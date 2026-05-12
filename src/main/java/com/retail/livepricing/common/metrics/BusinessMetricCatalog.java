package com.retail.livepricing.common.metrics;

public final class BusinessMetricCatalog {
    private BusinessMetricCatalog() {
    }

    public static final String TICKS_PUBLISHED_TOTAL = "business.ticks_published_total";
    public static final String TICKS_REJECTED_TOTAL = "business.ticks_rejected_total";

    public static final String CONFLATION_INPUT_TOTAL = "business.conflation_input_total";
    public static final String CONFLATION_PUBLISHED_TOTAL = "business.conflation_published_total";
    public static final String CONFLATION_DROPPED_TOTAL = "business.conflation_dropped_total";

    public static final String PRICE_UPDATES_PUBLISHED_TOTAL = "business.price_updates_published_total";
    public static final String STALE_UPDATES_TOTAL = "business.stale_updates_total";

    public static final String IMPACTED_USERS_PER_TICK = "business.impacted_users_per_tick";
    public static final String ZERO_IMPACT_TICKS_TOTAL = "business.zero_impact_ticks_total";
    public static final String PORTFOLIO_CALC_TASKS_CREATED_TOTAL = "business.portfolio_calc_tasks_created_total";
    public static final String PORTFOLIO_CALCULATIONS_TOTAL = "business.portfolio_calculations_total";

    public static final String PORTFOLIO_SNAPSHOTS_PUBLISHED_TOTAL = "business.portfolio_snapshots_published_total";
    public static final String PORTFOLIO_MISSING_PRICE_TOTAL = "business.portfolio_missing_price_total";

    public static final String TICK_TO_SCREEN_LATENCY_MS = "business.tick_to_screen_latency_ms";
    public static final String PORTFOLIO_RECALC_DELAY_MS = "business.portfolio_recalc_delay_ms";
    public static final String RESUME_CATCHUP_MS = "business.resume_catchup_ms";

    public static final String UPDATES_DELIVERED_TOTAL = "business.updates_delivered_total";
    public static final String UPDATES_FAILED_TOTAL = "business.updates_failed_total";

    public static final String WS_CONNECTIONS_OPENED_TOTAL = "business.ws_connections_opened_total";
    public static final String WS_CONNECTIONS_CLOSED_TOTAL = "business.ws_connections_closed_total";
    public static final String WS_GAP_EVENTS_TOTAL = "business.ws_gap_events_total";
}
