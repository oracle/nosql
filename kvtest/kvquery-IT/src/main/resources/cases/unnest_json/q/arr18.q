select /* FORCE_PRIMARY_INDEX(Foo) */
       $phone.areacode, min($phone.kind) as min
from foo as t, unnest(t.info.address.phones[] as $phone)
group by $phone.areacode
