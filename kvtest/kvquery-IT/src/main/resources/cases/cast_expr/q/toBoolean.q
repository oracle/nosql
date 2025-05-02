SELECT
    cast ( true as boolean ),
    cast ( "true" as boolean ),
    cast ( "True" as boolean ),
    cast ( "yes" as boolean ),
    cast ( "on" as boolean ),
    cast ( "1" as boolean ),
    cast ( "adevarat" as boolean )
from Foo