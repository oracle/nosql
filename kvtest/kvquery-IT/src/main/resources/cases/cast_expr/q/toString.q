SELECT
    cast ( true as string ),
    cast ( 1 as string ),
    cast ( 999999999999 as string ),
    cast ( 2.3 as string ),
    cast ( 2.30000000000001 as string ),
    cast ( cast ( "A" as enum(A, B, C) ) as string )
from Foo
limit 1
