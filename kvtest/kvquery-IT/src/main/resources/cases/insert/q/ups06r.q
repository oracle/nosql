#
# Row exits. SET TTL with new TTL value
#
upsert into foo $f values (
    "gtm", 
    3, 
    100, 
    {
      "a" : 10,
      "b" : 30 
    },
    default
)
set ttl 53 hours
returning $f as row, remaining_days($f) as ttl
