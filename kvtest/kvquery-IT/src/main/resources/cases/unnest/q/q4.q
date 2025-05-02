select id, lastName, $phone as phone
from foo as $t, $t.address.phones[] as $phone
