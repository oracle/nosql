#Expression is a case Expressionession with is null in predicate
select id from sn s where s.address.state="CA" and (case when s.address.city>"B" then s.age>10 else exists s.firstName end) is null