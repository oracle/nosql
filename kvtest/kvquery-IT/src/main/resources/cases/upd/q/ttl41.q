update NoTTL $f
set ttl case 
        when remaining_hours($f) < 0 then 2
        else 10
        end hours
where id = 41

select
  case when remaining_hours($f) = 2 then
         true
       else
         remaining_hours($f)
  end
from NoTTL $f
where id = 41
