#
# Row exits. SET TTL with an empty TTL value
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
set ttl seq_concat() days
returning $f as row, remaining_days($f) as ttl
