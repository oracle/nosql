select lastName,
       [ $C.address.phones[$element.work > $[2].work] ],
       true as bool
from Complex $C
