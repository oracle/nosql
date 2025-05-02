#expression returns week using For Atomic String using Cast.
#Negative Case, Invalid String
SELECT id,extract(week from cast(t.s as timestamp)) FROM Extract t WHERE id=3 
