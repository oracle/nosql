select d.company_id, t.team_id, e.emp_id
from company.department.team.employee e left outer join company.department.team t on e.company_id = t.company_id and e.department_id = t.department_id and e.team_id = t.team_id, company.department d
where d.company_id = e.company_id and
      d.department_id = e.department_id
order by d.company_id, t.team_id, e.emp_id