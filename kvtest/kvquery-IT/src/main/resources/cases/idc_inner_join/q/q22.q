select c.company_id
from company c, company.department d, company.reviews r, company.department.team t, company.project p, company.department.team.employee e
where c.company_id = d.company_id and
       d.company_id = r.company_id and
       r.company_id = t.company_id and
       t.company_id = p.company_id and
       p.company_id = e.company_id