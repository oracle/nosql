select $phone.areacode, count(*) as cnt
from foo $f, unnest(row_metadata($f).address.phones[] as $phone)
group by $phone.areacode

#select $phone.areacode, count(*) as cnt
#from foo $f, unnest($f.info.address.phones[] as $phone)
#group by $phone.areacode
