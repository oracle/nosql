# partial key + filtering
select id
from Foo f
where f.info.address.state = "CA" and f.info.age >= 10
