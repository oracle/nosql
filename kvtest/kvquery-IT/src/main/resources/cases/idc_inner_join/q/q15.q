select c.company_id, t.team_id, size(t.technologies_used) as num_tech
from company c, company.department.team t
where c.company_id = t.company_id