select $d.company_id, $d.department_id, $budget_sector, $nr.record_id, $nr.value
from company.department $d, company.null_records $nr, $d.budget_breakdown.keys() as $budget_sector
where $d.company_id = $nr.company_id and
      $nr.value is not null and
      ( $nr.value = 2147483647 or $nr.value = -2147483648)
order by $d.company_id, $d.department_id, $nr.record_id