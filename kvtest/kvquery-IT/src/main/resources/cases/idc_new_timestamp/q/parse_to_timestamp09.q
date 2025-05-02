# test for abbreviated time zone id

select
parse_to_timestamp("Nov 26, 2021 21-50-30.999999 PST","MMM dd, yyyy HH-mm-ss.SSSSSS zzz")
from roundFunc where id=6