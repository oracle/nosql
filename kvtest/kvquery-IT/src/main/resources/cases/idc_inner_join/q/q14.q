select distinct c.company_id
from company c, company.department d
where c.company_id = d.company_id
order by c.company_id desc