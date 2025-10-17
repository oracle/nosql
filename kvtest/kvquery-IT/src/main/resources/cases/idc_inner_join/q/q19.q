select c.company_id, c.head_office_location, nr.record_id, nr.value
from company c, company.null_records nr
where c.company_id = nr.company_id and
      nr.value is null and
      c.head_office_location is not null
order by c.company_id

