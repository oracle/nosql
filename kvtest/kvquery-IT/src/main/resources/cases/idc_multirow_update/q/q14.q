update jsoncol $j
put $j {"address":{"city":"Burlington", "State":"MA"}},
set firstThread = true,
remove index,
set ttl 8 days
where majorKey1 = "k1" and majorKey2 = "k2"

select $j, remaining_days($j) as remaining_days
from jsoncol $j
where majorKey1 = "k1" and majorKey2 = "k2"