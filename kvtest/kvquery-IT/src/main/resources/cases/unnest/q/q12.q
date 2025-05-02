select /*+ FORCE_PRIMARY_INDEX(Foo) */ id
from foo as $t, $t.address.phones[] as $phone, $t.children.Anna as $anna
where $anna.age < 20
