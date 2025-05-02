#
# any compararison with heterogeneous sequences
#
select 1
from boo b
where b.info.array[] =any 3
