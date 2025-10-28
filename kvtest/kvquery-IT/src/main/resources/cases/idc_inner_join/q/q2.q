select d.company_id, d.department_id, d.name
from company.department d, company c
where c.company_id = d.company_id