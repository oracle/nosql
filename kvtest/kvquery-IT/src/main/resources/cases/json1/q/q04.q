#
# Slice step:
# Input : array, record
#
# Array constructor
# Input: map, empty, record
#
select [ case
         when id < 2 then f.info.address.phones[2:10]
         when id < 5 then f.record[:2]
         else f.record[2:]
         end
       ]
from foo f

