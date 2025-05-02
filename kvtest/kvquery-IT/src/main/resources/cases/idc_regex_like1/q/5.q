#regex_like function with Update clause

update playerinfo.desc p 
set p.desc="updated successfully" 
where regex_like(p.desc,".*clause.*") 
and id3=2 and id=2 and id1=3
