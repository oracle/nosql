#Expression is a case Expressionession with is null in projection
select id,[ case when s.children.John.age=10 then s.children.Lisa.age=13 else exists s.children end] is  null from sn s