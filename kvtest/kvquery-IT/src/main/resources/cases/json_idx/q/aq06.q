# partial key + filtering
select /*+ FORCE_INDEX(Foo idx_state_areacode_age) */ id
from Foo f
where f.info.address.state = "CA" and f.info.age >= 10
