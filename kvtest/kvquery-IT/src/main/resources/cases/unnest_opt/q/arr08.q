select $phone.areacode, avg(age) as age
from foo as $t, $t.address.phones[] as $phone
group by $phone.areacode
