select c.company_id, d.department_id, t.team_id
from company c, company.department d left outer join company.department.team t
on d.company_id = t.company_id and d.department_id = t.department_id
where c.company_id = d.company_id