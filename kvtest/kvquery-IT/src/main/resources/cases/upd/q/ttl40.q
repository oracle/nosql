update NoTTL $f
set ttl case 
        when remaining_hours($f) < 0 then 2
        else 10
        end hours
where id = 40
returning 
  case when remaining_hours($f) = 2 then
         true
       else
         remaining_hours($f)
  end
