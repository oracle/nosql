select e.company_id, e.emp_id, r.review_id
from company.department.team.employee e, company.reviews r
where r.company_id = e.company_id
order by e.company_id, e.emp_id, r.review_id