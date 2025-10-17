select $phone.areacode, count(*) as cnt
from foo $f, row_metadata($f).address.phones[] as $phone
group by $phone.areacode
