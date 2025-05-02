delete from foo $f
where row_storage_size($f) > 200

select id,
       31 <= row_storage_size($f) and row_storage_size($f) <= 45 as row_size
from foo $f
order by id
