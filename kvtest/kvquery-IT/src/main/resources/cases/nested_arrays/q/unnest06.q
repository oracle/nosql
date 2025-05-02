select /* FORCE_PRIMARY_INDEX(bar) */id
from bar as $t, unnest($t.info.addresses[] as $address, $address.phones[][][] as $phone)
where $phone.areacode = 400 and $address.state > "C"
