update Foo $f
set ttl remaining_days($f) + 2 days
where id = 6
returning 
  case when remaining_days($f) = 7 then
         true
       else
         remaining_days($f)
  end
