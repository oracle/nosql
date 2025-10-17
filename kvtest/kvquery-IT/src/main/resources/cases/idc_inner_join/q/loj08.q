select c.company_id, e.emp_id, r.review_id
from company.reviews r, company c left outer join company.department.team.employee e on c.company_id = e.company_id and size(e.projects) >= size(e.skills)
where r.emp_id = e.emp_id and
      c.company_id = r.company_id

order by c.company_id, e.emp_id, r.review_id

