
select lastName, C.address.phones[size($) :]
from Complex C
