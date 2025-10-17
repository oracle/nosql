update A a
set ida2 = -9223372036854775808,
add a.ida3 5 "!@#$%^&*()",
set ida4 = -1.0 / 0.0,
put a.ida5 {"new_entry": cast('NaN' as double)}
where ida1 = "dfhvbjbdk b vjk        vdbvjkdfbvkdbfb   "

select *
from A
where ida1 = "dfhvbjbdk b vjk        vdbvjkdfbvkdbfb   "