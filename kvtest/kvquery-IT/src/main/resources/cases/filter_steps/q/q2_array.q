select lastName,
       [ C.address.phones[$[1].home = 11] ]
from Complex C
