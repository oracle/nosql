#expression returns month using For Atomic String using Cast.
#Negative Case, Invalid String
SELECT id,extract(month from cast(t.s as timestamp)) FROM Extract t WHERE id=3 
