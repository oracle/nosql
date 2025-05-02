select
    cast ( "A" as enum(A, B, C) ),
    cast ( "B" as enum(A, B, C) ),
    cast ( "C" as enum(A, B, C) )
from Foo