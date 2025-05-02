# Test Description: For High use implicit variables $pos.

select  $C.address.phones[1 : $pos]
from Complex $C
