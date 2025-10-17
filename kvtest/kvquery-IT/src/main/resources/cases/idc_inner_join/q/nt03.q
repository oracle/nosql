select c.company_id, t.team_id, e.emp_id
from nested tables(company c descendants(company.department.team t, company.reviews r)), company.department.team.employee e
where t.company_id = e.company_id and
      t.team_id = e.team_id and
      r.emp_id = e.emp_id
order by c.company_id desc, t.team_id, e.emp_id