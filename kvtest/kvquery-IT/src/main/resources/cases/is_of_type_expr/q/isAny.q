select
    [ 1, "abc", TRUE, False, [1], {}, NULL][:] IS OF TYPE ( Any* ),
    [ 1, "abc", TRUE, False               ][:] IS OF TYPE ( AnyAtomic* ),
    [ 1, "abc", TRUE, False, [1]          ][:] IS OF TYPE ( AnyAtomic* ),
    [ 1, "abc", TRUE, False, {}           ][:] IS OF TYPE ( AnyAtomic* ),
    [ 1, "abc", TRUE, False, NULL         ][:] IS OF TYPE ( AnyAtomic* ),
    [ 1, "abc", True, False               ][:] IS OF TYPE ( AnyJsonAtomic* ),
    [ 1, "abc", True, False, [1]          ][:] IS OF TYPE ( AnyJsonAtomic* ),
    [ 1, "abc", True, False, {}           ][:] IS OF TYPE ( AnyJsonAtomic* ),
    [ 1, "abc", True, False, NULL         ][:] IS OF TYPE ( AnyJsonAtomic* ),
//    [ {"a":1}, {}                       ][:] IS OF TYPE ( AnyRecord* ),
    [ {}, 1                               ][:] IS OF TYPE ( AnyRecord* ),
    [ {}, "abc"                           ][:] IS OF TYPE ( AnyRecord* ),
    [ {}, [1]                             ][:] IS OF TYPE ( AnyRecord* ),
    [ {}, NULL                            ][:] IS OF TYPE ( AnyRecord* )
FROM Foo
