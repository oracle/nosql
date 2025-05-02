select /* FORCE_PRIMARY_INDEX(Foo) */
       $phone.areacode, avg(t.record.long) as age
from foo as t, unnest(t.info.address.phones[] as $phone)
where t.info.address.phones[].areacode =any 500
group by $phone.areacode
