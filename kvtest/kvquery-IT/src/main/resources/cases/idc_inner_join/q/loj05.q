select c.company_id, d.department_id, e.emp_id, r.review_id
from company c left outer join company.department d on c.company_id = d.company_id left outer join company.department.team.employee e on d.company_id = e.company_id and d.department_id = e.department_id, company.reviews r
where c.company_id = r.company_id and
      e.emp_id = r.emp_id
order by c.company_id, d.department_id, e.emp_id, r.review_id