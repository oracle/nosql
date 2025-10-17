select c.company_id, c.name, p.project_id, $progress
from company.project p, company.client c, unnest(p.project_milestones.values() as $progress)
where p.company_id = c.company_id and
      p.client_id = c.client_id and
      "Email" in c.preferred_contact_methods[]
order by c.company_id, c.client_id, p.project_id
