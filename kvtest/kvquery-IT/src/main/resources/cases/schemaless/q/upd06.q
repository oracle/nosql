update jsoncol $j
set $j.address.name = "JAIN",
set $j.index = 9970001
where majorKey1 = "cc" and majorKey2 = "ib" and minorKey = "min1"
returning $j, remaining_days($j)
