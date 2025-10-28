select c.company_id, d.department_id, e.emp_id
from company c, company.department d left outer join company.department.team.employee e
on d.company_id = e.company_id and d.department_id = e.department_id
where c.company_id = d.company_id