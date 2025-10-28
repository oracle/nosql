select c.company_id, d.company_id as dept_comp_id, d.department_id, e.emp_id, e.team_id as emp_team_id, t.team_id
from nested tables(company.department.team.employee e ancestors(company.department d, company c)), company.department.team t
where t.company_id = c.company_id and
      e.team_id = t.team_id
order by c.company_id, d.company_id, d.department_id, t.team_id, e.emp_id