select lastName,
       [ $C.address.phones[$ and $element.work > 501] ]
from Complex $C
