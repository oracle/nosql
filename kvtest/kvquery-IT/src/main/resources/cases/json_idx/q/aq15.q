# partial key
select id, f.info.address.state
from Foo f
where f.info.address.state = "CA" and f.info.address.phones[].areacode =any 650
