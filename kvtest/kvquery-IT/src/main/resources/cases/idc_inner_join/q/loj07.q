select t.company_id, t.department_id, t.team_id, e.emp_id, p.project_id
from company.project p, company.department.team t left outer join company.department d on t.company_id = d.company_id and t.department_id = d.department_id left outer join company.department.team.employee e on t.company_id = e.company_id and t.department_id = e.department_id and t.team_id = e.team_id
where p.company_id = t.company_id and
      p.project_id in e.projects[]
order by t.company_id, t.department_id, t.team_id, e.emp_id, p.project_id