select t.company_id, d.department_id, t.team_id, e.emp_id, r.review_id
from company.reviews r, nested tables(company.department.team t ancestors(company.department d on d.established > cast("2016-10-20" AS TIMESTAMP)) descendants(company.department.team.employee e on size(e.projects) > 1))
where r.company_id = e.company_id and
      r.emp_id = e.emp_id
order by t.company_id, d.department_id, t.team_id, e.emp_id, r.review_id