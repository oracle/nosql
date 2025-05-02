#timestamp is long with valid pattern

select
l3,format_timestamp(l3,"yyyy-MM-dd"),
l3,format_timestamp(l3,"HH:mm:ssXXXXX"),
l3,format_timestamp(l3,"yyyy-MM-dd'T'HH:mm:ss[XXXXX][XXXXX]")
from roundFunc where id=4