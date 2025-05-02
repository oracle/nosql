#
# Explicit $phone
#
select id
from foo as $t, $t.address as $addr, $addr.phones[] as $phone, $addr.state as $state
where $state = "MA"
