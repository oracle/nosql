# partial key + filtering
select /*+ FORCE_INDEX(Foo idx_state_areacode_age) */ id
from Foo f
where f.info.address.state IS NULL and
      f.info.age IS NULL
