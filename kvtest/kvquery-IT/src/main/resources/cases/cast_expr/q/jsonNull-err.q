SELECT
    cast ( [1, NULL, 999999999999, 3.3, "123"][:] as integer* )
FROM Foo