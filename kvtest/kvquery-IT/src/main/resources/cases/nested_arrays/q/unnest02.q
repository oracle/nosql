select /* FORCE_PRIMARY_INDEX(bar) */id, $phone.areacode
from bar as $t, unnest($t.info.addresses[].phones[][][] as $phone)
order by $phone.areacode, id
