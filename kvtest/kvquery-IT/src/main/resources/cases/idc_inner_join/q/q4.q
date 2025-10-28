select e.company_id, e.emp_id, e.name
from company.department.team.employee e, company c
where c.company_id = e.company_id
order by e.company_id, e.emp_id