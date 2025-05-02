# Array filter step
# Test Description: Test with invalid values of implicit variables for arrays for $element. Like $elements etc.

select $C.address.phones[$elements.work > 501 and $poss > 3]
from Complex $C
