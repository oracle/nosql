# partial key
select id, f.record.int
from Foo f
where f.info.address.state = "CA" and f.info.address.phones.areacode[] =any 650
