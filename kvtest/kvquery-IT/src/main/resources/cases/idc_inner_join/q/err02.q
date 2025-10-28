select d.company_id, d.department_id, d.name
from company c, company.department d on department_id > 1
where c.company_id = d.company_id