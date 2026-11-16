-- Fails closed: any partition whose row counts or checksums do not balance
-- between raw, bronze, staging and fact.
select *
from {{ ref('ops_reconciliation') }}
where not is_balanced
