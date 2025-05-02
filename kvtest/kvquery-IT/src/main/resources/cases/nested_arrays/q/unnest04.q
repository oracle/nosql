select /* FORCE_PRIMARY_INDEX(bar) */id
from bar as $t, unnest($t.info.addresses[] as $address, $address.phones[][][] as $phone)
where $phone.areacode = 600 and $address.state = "CA"
