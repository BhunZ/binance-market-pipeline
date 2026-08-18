-- Exactly one current version per symbol.
--
-- Two open validity windows for the same pair means every fact in the overlap joins twice and
-- every measure doubles — the classic way a Type 2 dimension corrupts a warehouse. It produces
-- no error, no null, and no row-count anomaly at load time; it shows up as volumes that are
-- exactly twice what the exchange reports, which is very hard to trace back to a join.

select symbol, count(*) as open_versions
from {{ ref('dim_symbol') }}
where is_current
group by symbol
having count(*) <> 1
