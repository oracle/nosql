select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as t,
            unnest(t.info.address.phones[] as $phone),
            t.info.children.values() as $child
where t.info.address.state = "CA" and
      $phone.areacode = 650 and
      $child.age > 7
