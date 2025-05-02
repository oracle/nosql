#
# Elimination of $addr
#
select id, $phone
from foo as $t, $t.address.phones[] as $phone, $t.address as $addr
where $addr.state = "MA"
