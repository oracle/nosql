select /* FORCE_PRIMARY_INDEX(bar) */
       $address.state, count($phone.kind) as cnt
from bar as $t, unnest($t.info.addresses[] as $address, $address.phones[][][] as $phone)
where $address.state > "C"
group by $address.state
