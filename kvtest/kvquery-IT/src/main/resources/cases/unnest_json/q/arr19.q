select /* FORCE_PRIMARY_INDEX(Foo) */
       $phone.areacode, $phone.kind, count(*) as cnt
from foo as t, unnest(t.info.address.phones[] as $phone)
group by $phone.areacode, $phone.kind
