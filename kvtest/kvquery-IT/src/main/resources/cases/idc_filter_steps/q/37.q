# Map filter step
# Test Description: Test with invalid values of implicit variables for maps for $keys and $values. Like $keyss and $valuess etc.

select C.children($keyss = "Lisa" or $Valuess = "test")
from Complex C
