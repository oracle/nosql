SELECT
    id,
    cast ( str as string ) ,
    cast ( lng as integer ),
    cast ( Foo.map.key1[1] as double* )
FROM Foo