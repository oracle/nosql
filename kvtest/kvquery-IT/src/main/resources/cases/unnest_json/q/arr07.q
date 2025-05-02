select /* FORCE_PRIMARY_INDEX(Foo) */
       $phone.areacode, count(*) as age
from foo as t, unnest(t.info.address.phones[] as $phone)
group by $phone.areacode
