SELECT
    id,
    cast ( Foo.map.key1[1] as double )
FROM Foo