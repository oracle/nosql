select id, 
       case 
       when u.info.address.phones is of type (array(json)) 
       then size(u.info.address.phones)
       when u.info.address.phones != null
       then 1
       else 0
       end as num_phones
from foo u 
order by id
