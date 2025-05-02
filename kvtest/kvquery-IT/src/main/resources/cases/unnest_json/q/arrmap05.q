select /* FORCE_PRIMARY_INDEX(Foo) */id
from foo as t,
            t.info.address.phones[] as $phone,
            unnest(t.info.children.values() as $child)
where $phone.kind > "h" and
      $child.school > "sch_1" and $child.school < "sch_3"
