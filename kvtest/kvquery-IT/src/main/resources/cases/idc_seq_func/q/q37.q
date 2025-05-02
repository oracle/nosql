#Test: Expression using Sequence transform expression

select id1, 
       seq_sum(seq_transform(p.json.stats[], $.runs / $.mactches)) as runspermatch
from playerinfo p
where id1=10