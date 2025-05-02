update Foo $f
set ttl remaining_hours($f) + 13 hours
where id = 5
returning 
  case when 133 <= remaining_hours($f) and remaining_hours($f) < 157 then
         true
       else
         remaining_hours($f)
  end
