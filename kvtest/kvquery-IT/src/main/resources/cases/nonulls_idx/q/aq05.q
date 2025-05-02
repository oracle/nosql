# partial key + filtering
select id
from Foo f
where f.info.address.state IS NULL and
      f.info.age IS NULL
