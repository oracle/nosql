SELECT
  id,
  cast (cast ( f.map.values() as array(long)* ) as any*) 
FROM Foo f
