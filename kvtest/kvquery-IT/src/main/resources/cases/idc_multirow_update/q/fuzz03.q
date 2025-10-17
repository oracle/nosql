update A a
set ida2 = 4793594583598,
add a.ida3 1 "!@#$%^&*()",
set ida4 = cast('NaN' as double),
put a.ida5 {"new_entry": cast('NaN' as double)}
where ida1 = "&`b|nm./^*@!\u6F22\u5B57 \uD83D\uDE00\u6F22"

select *
from A
where ida1 = "&`b|nm./^*@!\u6F22\u5B57 \uD83D\uDE00\u6F22"