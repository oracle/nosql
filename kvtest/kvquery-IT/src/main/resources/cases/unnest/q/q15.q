select id, $children.Mark
from foo as $t, $t.address.phones[] as $phone, $t.children as $children
where $children.keys() =any "Anna"
