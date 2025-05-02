SELECT
  cast ( [][:] as integer? ),
  cast ( [][:] as integer* ),
  cast ( [1][:] as integer? ),
  cast ( [1][:] as integer ),
  cast ( [1][:] as integer* ),
  cast ( [1][:] as integer+ ),
  cast ( [1, 2, 3][:] as integer* ),
  cast ( [1, 2, 3][:] as integer+ )
FROM Foo

