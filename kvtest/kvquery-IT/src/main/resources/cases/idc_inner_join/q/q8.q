select e.company_id, d.department_id, count(*) as count
from company.department.team.employee e, company.department d, company.reviews r
where e.company_id = d.company_id and e.company_id = r.company_id
group by e.company_id, d.department_id
order by e.company_id, d.department_id desc