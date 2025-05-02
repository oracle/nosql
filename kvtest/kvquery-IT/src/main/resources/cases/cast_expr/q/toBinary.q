SELECT
    cast ( cast ( "test" as binary ) as string),
    cast ( cast ( "test" as binary(3)) as string),
    cast ( "test" as binary ),
    cast ( "test" as binary(3))
FROM Foo