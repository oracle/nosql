select case
       when f.uid3 is of type (string as uuid) then cast(f.uid3 as string)
       else f.uid3
       end as uid3updated
from bar1 f
