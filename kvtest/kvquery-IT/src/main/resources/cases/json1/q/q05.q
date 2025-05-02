#
# Slice step:
# Input : array, record
# NULL from boundary expr
#
# Array constructor
# Input: map, empty, atomics, record
#
select [ case
         when id < 2 then f.info.address.phones[size($) - 3:]
         when id < 4 then f.record[size($) - 3:]
         else f.record[$[0].long : $[0].long + 2]
         end
       ]
from foo f


