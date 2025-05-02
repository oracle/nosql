# Array filter step
# Test Description: Test with invalid values of implicit variables for arrays for $pos. Like $psos etc

select id, $C.arrint[$poss = 150 ]
from Complex $C
