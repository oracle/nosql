# partial key and range; only one multi-key pred pushed
select id
from Foo f
where f.info.address.state = "CA" and
      f.info.address.phones.areacode >any 500 and
      f.info.address.phones.areacode <any 600
