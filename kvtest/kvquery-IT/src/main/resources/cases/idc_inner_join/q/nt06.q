select c1.company_id, e.emp_id, p.project_id
from nested tables(company c1 descendants(company.department.team.employee e)), nested tables(company c2 descendants(company.project p on p.project_milestones.Phase1 = "Completed")
)
where c1.company_id = c2.company_id and
      p.project_id in e.projects[]
order by c1.company_id, e.emp_id, p.project_id
