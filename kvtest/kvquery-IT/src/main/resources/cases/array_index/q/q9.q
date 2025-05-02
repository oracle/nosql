#
# partial key and range; only one multi-key pred pushed, the other ia
# always true. Same as q8, but with the positions of the preds swapped.
#  
select id
from Foo t 
where t.rec.a = 10 and t.rec.c.ca >any 6 and t.rec.c.ca >=any 6
