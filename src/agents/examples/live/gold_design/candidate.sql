CREATE VIEW candidate.report AS
SELECT
    COALESCE(building, 'Unknown') AS building,
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) AS terminal_tickets,
    COUNT(*) FILTER (WHERE status IN ('open', 'in_progress', 'pending_vendor', 'escalated')) AS unresolved_tickets,
    COUNT(*) FILTER (WHERE status IS NULL) AS unknown_status_tickets,
    COUNT(*) FILTER (
        WHERE LOWER(BTRIM(priority)) = 'critical'
          AND status IN ('open', 'in_progress', 'pending_vendor', 'escalated')
    ) AS critical_unresolved_tickets,
    COUNT(cost) AS tickets_with_recorded_cost,
    SUM(cost) AS total_recorded_cost,
    AVG(cost) AS average_recorded_cost,
    100.0 * COUNT(cost) / NULLIF(COUNT(*), 0) AS cost_coverage_pct,
    100.0 * COUNT(*) FILTER (WHERE status IN ('resolved', 'closed'))
        / NULLIF(COUNT(*) FILTER (
            WHERE status IN ('open', 'in_progress', 'pending_vendor', 'escalated', 'resolved', 'closed')
        ), 0) AS terminal_rate_pct
FROM silver.tickets
GROUP BY COALESCE(building, 'Unknown')