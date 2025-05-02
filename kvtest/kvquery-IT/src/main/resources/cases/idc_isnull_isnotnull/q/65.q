#Expression returns Json Atomic Types with jnull in predicate
select id,s.children."B.Balance" from sn s where s.children."B.Balance"=null