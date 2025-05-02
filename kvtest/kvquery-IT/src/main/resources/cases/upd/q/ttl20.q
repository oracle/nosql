update Bar $f
set ttl remaining_hours($f) + 2 hours
where id = 20
returning 
  case when remaining_hours($f) = 32 then
         true
       else
         remaining_hours($f)
  end
