select e.company_id, e.emp_id, e.name
from company c, company.department.team.employee e
where c.company_id = e.company_id
order by e.company_id, e.emp_id