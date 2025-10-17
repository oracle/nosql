select d.company_id, d.department_id, d.name
from company c, company.department d
where c.company_id = d.company_id