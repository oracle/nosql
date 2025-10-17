select t.company_id, t.team_id, count(*) as count, sum(s.skill_value) as total_skill, min(s.skill_value) as min, max(s.skill_value) as max
from company.department.team t, company.department.team.employee e, company.skill s, unnest(e.skills[] as $s)
where t.company_id = e.company_id and
      e.company_id = s.company_id and
      t.team_id = e.team_id and
      $s = s.skill_id
group by t.company_id, t.team_id