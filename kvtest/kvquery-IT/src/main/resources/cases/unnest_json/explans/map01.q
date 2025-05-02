select id, $children.Mark
from foo as $t, $t.info.address.phones[] as $phone, $t.info.children as $children
where $children.keys() =any "Anna"
