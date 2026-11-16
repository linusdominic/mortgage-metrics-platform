-- Usable LAR rows. Rejected rows go to quarantine.rejected_lar_rows instead.
select * exclude (reject_reason)
from {{ ref('stg_hmda__lar_typed') }}
where reject_reason is null
