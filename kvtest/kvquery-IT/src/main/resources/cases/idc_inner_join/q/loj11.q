select r.company_id, d.department_id, e.emp_id, r.review_id
from company.reviews r, company c2 left outer join company.department d on c2.company_id = d.company_id, nested tables(company c3 descendants(company.department.team.employee e))
where r.company_id = c2.company_id and
      c2.company_id = c3.company_id and
      r.emp_id = e.emp_id and
      d.department_id = e.department_id
order by r.company_id, d.department_id, e.emp_id
