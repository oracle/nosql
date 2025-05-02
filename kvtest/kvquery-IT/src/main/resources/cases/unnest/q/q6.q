select id, age, $child, $phone.work
from foo as $t, $t.address.phones[] as $phone, $t.children.keys() as $child
where $t.address.state = "MA" or $t.address.state = "NY"
