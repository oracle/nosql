select id, 
       f.info.address.phones[$pos = 
                             case when exists f.info.address then 0 
                                  when exists f.info.children then 1 
                                  else 3 end]
from foo f 
order by id
