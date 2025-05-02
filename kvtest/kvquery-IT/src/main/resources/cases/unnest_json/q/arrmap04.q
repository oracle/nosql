select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as t,
            unnest(t.info.address.phones[] as $phone),
            t.info.children.values() as $child
where $phone.areacode > 500 and
      $child.school > "sch_1" and $child.school < "sch_3"
