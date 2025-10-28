select c.company_id, d.department_id, year(established) as year, month(established) as month
from company c, company.department d
where c.company_id = d.company_id and
      established > cast("2016-09-01" AS TIMESTAMP) and
      established < cast("2024-04-05" AS TIMESTAMP)
order by c.company_id desc, department_id desc
