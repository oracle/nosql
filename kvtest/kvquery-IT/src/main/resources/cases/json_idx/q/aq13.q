#
# partial key and range; only one multi-key pred pushed, the other
# is always true
#  
select id
from Foo f
where f.info.address.state = "CA" and
      f.info.address.phones.areacode >any 650 and
      f.info.address.phones.areacode >=any 650
