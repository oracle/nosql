select e.company_id, e.emp_id, e.name as emp_name, s.name as skill
from company.department.team.employee e, company.skill s
where e.company_id = s.company_id and
      s.skill_id in e.skills[]
order by e.company_id, emp_id