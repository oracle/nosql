# partial key and range
select id
from Foo f
where f.info.address.state = "CA" and f.info.address.phones.areacode >any 500
