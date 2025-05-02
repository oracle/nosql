
select lastName, [ C.address.phones[-1:6] ]
from Complex C
