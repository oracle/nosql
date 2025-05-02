#TestDescription: ".*or.*"  Finds any values that have "or" in any position

select id1,p.name from playerinfo p where regex_like(p.name,".*or.*","i")