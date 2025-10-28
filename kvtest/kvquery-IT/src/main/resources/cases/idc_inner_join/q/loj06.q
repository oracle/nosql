select c.company_id, d.department_id, e.emp_id, r.review_id
from company.reviews r, company.department.team.employee e left outer join company c on e.company_id = c.company_id left outer join company.department d on e.company_id = d.company_id and e.department_id = d.department_id
where c.company_id = r.company_id and
      e.emp_id = r.emp_id
order by c.company_id, d.department_id, e.emp_id, r.review_id