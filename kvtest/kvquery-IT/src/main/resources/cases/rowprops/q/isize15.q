select partition($f) as part,
       count(*) as cnt, 
       avg(index_storage_size($f, "idx_city_phones")) as index_size
from foo $f
group by partition($f)
