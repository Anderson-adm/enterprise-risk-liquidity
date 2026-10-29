{{ config(severity='warn') }}
-- Completeness warning (not blocking): an active portfolio has no valid positions for the
-- reference date, e.g. its XML statement was quarantined. Visible in run_results / run report.
select p.portfolio_id, p.position_source_system
from {{ ref('silver_portfolio') }} p
where p.status = 'ACTIVE'
  and not exists (
      select 1 from {{ ref('silver_position') }} s
      where s.portfolio_id = p.portfolio_id and s.reference_date = {{ reference_date() }})
