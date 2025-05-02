compiled-query-plan

{
"query file" : "upd/q/upd20.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "UPDATE_ROW",
      "indexes to update" : [  ],
      "update clauses" : [
        {
          "iterator kind" : "PUT",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "map1",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "record",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "MAP_CONSTRUCTOR",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : "rec2"
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "f1"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [

                    ]
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "f2"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "j1"
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [

                        ]
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "j2"
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : 1
                          },
                          {
                            "iterator kind" : "CONST",
                            "value" : 3
                          }
                        ]
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "key3"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "foo"
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : "rec1"
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "f1"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "i1"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 11
                      }
                    ]
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "f2"
                  },
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "f2",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "rec1",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$"
                      }
                    }
                  }
                ]
              }
            ]
          }
        }
      ],
      "update TTL" : false,
      "isCompletePrimaryKey" : true,
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$$f",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":19},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "f",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        ]
      }
    },
    "FROM variable" : "$$f",
    "SELECT expressions" : [
      {
        "field name" : "map1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "map1",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "record",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      }
    ]
  }
}
}
