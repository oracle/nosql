compiled-query-plan

{
"query file" : "upd/q/upd35.q",
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
          "iterator kind" : "SET",
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
                "value" : "foo"
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
                        "value" : "f1f1"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 1
                      }
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
                        "value" : "f2f1"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 4
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "f2f2"
                      },
                      {
                        "iterator kind" : "FUNC_PARSE_JSON",
                        "input iterator" :
                        {
                          "iterator kind" : "PROMOTE",
                          "target type" : "String",
                          "input iterator" :
                          {
                            "iterator kind" : "EXTERNAL_VAR_REF",
                            "variable" : "$json"
                          }
                        }
                      }
                    ]
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
              "equality conditions" : {"id":29},
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
        "field name" : "f",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$f"
        }
      }
    ]
  }
}
}
