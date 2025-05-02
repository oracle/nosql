select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as t,
            t.info.address.phones[] as $phone,
            unnest(t.info.children.values() as $child)
where $phone.areacode = 500 and
      $child.age = 9 and
      $child.school > "a"
