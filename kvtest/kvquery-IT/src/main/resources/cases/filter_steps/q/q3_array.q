select lastName,
       [ C.address.phones[C.lastName = "last2"] ]
from Complex C
