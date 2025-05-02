select $areacode, avg(age) as age
from foo as $t, seq_distinct($t.address.phones[].areacode) as $areacode
group by $areacode
