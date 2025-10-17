select c1.company_id, r.review_id, e.emp_id
from company c1 left outer join company.reviews r on c1.company_id = r.company_id, company c2 left outer join company.department.team.employee e on c2.company_id = e.company_id
where c1.company_id = c2.company_id and
      r.emp_id = e.emp_id
order by c1.company_id, r.review_id, e.emp_id

