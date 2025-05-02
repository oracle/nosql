#
# Explicit $phone
#
select id
from foo as $t, $t.address as $addr, $addr.phones[] as $phone
where $addr.state = "MA"
