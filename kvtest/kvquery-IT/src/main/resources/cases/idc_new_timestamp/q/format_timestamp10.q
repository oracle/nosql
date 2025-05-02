#Invalid timezone: Shord IDs as input

select format_timestamp("2021-07-01T00:00:00","EEE, dd MMM yyyy HH:mm:ss zzzz",'AET')
from roundFunc where id=4