select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as t,
            unnest(t.info.address.phones[] as $phone),
            t.info.children.values() as $child
where t.info.address.state = "MA" and
      $phone.areacode = 500 and
      $child.age > 8
