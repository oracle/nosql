select /* FORCE_PRIMARY_INDEX(Foo) */
       $phone.areacode, avg(t.record.long) as age
from foo as t, unnest(t.info.address.phones[] as $phone)
group by $phone.areacode
