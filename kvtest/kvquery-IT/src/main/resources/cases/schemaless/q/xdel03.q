delete
from jsoncol $j
where majorKey1 = "99"
returning $j, remaining_days($j)
