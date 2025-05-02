select  /*+ FORCE_INDEX(Foo idx_children_anna_age) */ id
from foo as $t, $t.address.phones[] as $phone, $t.children.Anna as $anna
where $anna.age < 20
