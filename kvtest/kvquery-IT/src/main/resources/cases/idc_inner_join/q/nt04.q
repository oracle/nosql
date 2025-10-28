select c.company_id, t.company_id as tcomp_id,  d.department_id, t.team_id, e.emp_id
from company c, nested tables(company.department.team t ancestors(company.department d) descendants(company.department.team.employee e))
where c.company_id = e.company_id
order by c.company_id, d.department_id, t.team_id, e.emp_id