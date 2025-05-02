#Expression returns is null with String in Record 
select id, s.children.values().Relative from sn s where age is null and s.address.state is null
