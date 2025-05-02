select id, 
       case
       when not exists b.info.address.state then "EMPTY"
       when b.info.address.state is null then "NULL"
       else case when 5.2 < b.info.address.state and
                      b.info.address.state < 5.4 then 5.3
                 else b.info.address.state
            end 
       end as state
from bar b
order by b.info.address.state, b.info.address.city, b.info.age
