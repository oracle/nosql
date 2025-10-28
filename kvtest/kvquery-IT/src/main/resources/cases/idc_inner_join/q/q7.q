select e.company_id, d.department_id, e.emp_id, e.name, r.review_id
from company.department.team.employee e, company.department d, company.reviews r
where e.company_id = d.company_id and
      e.company_id = r.company_id and
      d.department_id = e.department_id and
      r.emp_id = e.emp_id
order by e.company_id, d.department_id, e.emp_id, r.review_id