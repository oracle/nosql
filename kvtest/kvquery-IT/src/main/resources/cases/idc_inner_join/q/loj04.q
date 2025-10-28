select c.company_id, d.department_id, e.emp_id
from company.department.team.employee e left outer join company.department d on e.company_id = d.company_id and e.department_id = d.department_id, company c
where c.company_id = e.company_id
order by c.company_id, d.department_id, e.emp_id