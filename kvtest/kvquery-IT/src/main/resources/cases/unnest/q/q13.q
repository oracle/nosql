select id
from foo as $t, $t.address.phones[] as $phone, $t.children as $children
where $children.Anna.age < 20
