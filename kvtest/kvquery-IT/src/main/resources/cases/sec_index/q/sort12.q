select id,
       case
       when exists t.info.name is null then "NULL"
       when exists t.info.name then t.info.name
       else "EMPTY"
       end as name
from T2 t
order by t.info.name nulls first, t.id
