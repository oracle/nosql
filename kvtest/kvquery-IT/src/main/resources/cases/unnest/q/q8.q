select id, $phone
from foo as $t, $t.address.phones[] as $phone, $t.address as $addr
order by $addr.state
limit 3
offset 1
