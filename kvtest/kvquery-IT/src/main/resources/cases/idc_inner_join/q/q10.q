select e.company_id, e.emp_id, $proj as project_id, p.name as project_name, c.client_id, c.name as client_name
from company.department.team.employee e, company.project p, company.client c, unnest(e.projects[] as $proj)
where e.company_id = p.company_id and
      p.company_id = c.company_id and
      $proj = p.project_id and
      p.client_id = c.client_id
order by e.company_id, e.emp_id, c.client_id
