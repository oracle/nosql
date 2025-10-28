select e.company_id, e.emp_id, r.review_id
from company.reviews r, company.department.team.employee e
where r.company_id = e.company_id
order by e.company_id, e.emp_id, r.review_id