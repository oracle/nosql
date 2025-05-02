update Bar $f
set ttl remaining_days($f) + 2 days
where id = 21
returning
  case when remaining_days($f) = 3 then
         true
       else
         remaining_days($f)
  end
