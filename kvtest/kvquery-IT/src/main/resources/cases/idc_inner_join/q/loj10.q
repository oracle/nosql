select c1.company_id, d.department_id, t.team_id
from company c1 left outer join company.department d on c1.company_id = d.company_id, nested tables(company c2 descendants(company.department.team t))
where c1.company_id = c2.company_id and
      d.department_id = t.department_id
order by c1.company_id, d.department_id, t.team_id