#Expression returns jnull with is not null in predicate and projection
select id,s.children."B.Balance" is not null from sn s where s.children."B.Balance"=null is not null