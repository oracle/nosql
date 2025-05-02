select id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
where id <= 1
order by id
