select c.company_id, d.company_id as dept_comp_id, d.department_id, t.team_id, e.emp_id
from company c, nested tables(company.department d descendants(company.department.team t, company.department.team.employee e))
where c.company_id = e.company_id
order by c.company_id, d.company_id, d.department_id, t.team_id, e.emp_id