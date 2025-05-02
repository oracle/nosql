select
    cast ( [1, 999999999999, 3.3, "123"][:] as integer* )
from Foo