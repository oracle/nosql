select id, 
       f.info.keys(case 
                   when exists f.info.address.phones then f.info.address.phones 
                   when exists f.info.children then f.info.children 
                   end) 
from foo f 
where id=13 or id=16 or id=24 
order by id
