select id, $areacode
from foo as $t, $t.address.phones[].areacode as $areacode
order by id
