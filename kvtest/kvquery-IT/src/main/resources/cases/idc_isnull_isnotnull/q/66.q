#Expression returns jnull with is null in predicate
select id,s.children."B.Balance" from sn s where s.children."B.Balance"=null is null