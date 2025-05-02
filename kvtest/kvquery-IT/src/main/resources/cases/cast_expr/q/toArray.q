SELECT
  id,
  cast ( f.map.values(true) as array(long)* )
FROM Foo f

