SELECT
  id,
  cast ( rec as RECORD(fmap MAP(ARRAY(string))) )
FROM Foo
