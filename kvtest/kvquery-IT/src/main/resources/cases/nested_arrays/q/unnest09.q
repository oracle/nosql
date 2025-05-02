select /*+ FORCE_INDEX(bar idx_areacode_number_long) */id
from bar as $t, unnest($t.info.addresses[] as $address, $address.phones[][][] as $phone)
where $phone.areacode > 400 and $address.state = "MA"
