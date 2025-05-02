#
# Row exits. No SET TTL
#
upsert into foo values (
    "gtm", 
    2, 
    100, 
    {
      "a" : 10,
      "b" : 30 
    },
    default
)
