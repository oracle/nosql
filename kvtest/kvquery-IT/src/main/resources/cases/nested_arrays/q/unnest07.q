select /* FORCE_PRIMARY_INDEX(bar) */
       count($phone.number) as cnt,
       sum($phone.number) as sum
from bar as $t, unnest($t.info.addresses[] as $address, $address.phones[][][] as $phone)
where $address.state > "C"
group by $address.state
