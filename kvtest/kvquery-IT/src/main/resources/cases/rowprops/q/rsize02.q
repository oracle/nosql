select id,
       170 <= row_storage_size($f) and row_storage_size($f) <= 185
from foo $f
where $f.address.state = "MA"
