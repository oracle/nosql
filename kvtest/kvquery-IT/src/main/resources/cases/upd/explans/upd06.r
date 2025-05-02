compiled-query-plan

{
"query file" : "upd/q/upd06.q",
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
            "field name" : "children",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "SEQ_CONCAT",
            "input iterators" : [
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "Matt"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "age"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 14
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "school"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "sch_2"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "friends"
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : false,
                        "input iterators" : [
                          {
                            "iterator kind" : "CONST",
                            "value" : "Bill"
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "Dave"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : null
                  }
                ]
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "George"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "age"
                      },
                      {
                        "iterator kind" : "ARRAY_CONSTRUCTOR",
                        "conditional" : true,
                        "input iterators" : [
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "age",
                            "input iterator" :
                            {
                              "iterator kind" : "FIELD_STEP",
                              "field name" : "Tim",
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
                ]
              }
            ]
          }
        },
        {
          "iterator kind" : "SET",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "record",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "MAP_CONSTRUCTOR",
            "input iterators" : [

            ]
          }
        },
        {
          "iterator kind" : "JSON_MERGE_PATCH",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "MAP_CONSTRUCTOR",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : "address"
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "city"
                  },
                  {
                    "iterator kind" : "EXTERNAL_VAR_REF",
                    "variable" : "$city"
                  },
                  {
                    "iterator kind" : "PROMOTE",
                    "target type" : "String",
                    "input iterator" :
                    {
                      "iterator kind" : "EXTERNAL_VAR_REF",
                      "variable" : "$state"
                    }
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "CA"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "phones"
                  },
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "areacode"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 610
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "number"
                      },
                      {
                        "iterator kind" : "EXTERNAL_VAR_REF",
                        "variable" : "$number"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "kind"
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : "home"
                      }
                    ]
                  }
                ]
              },
              {
                "iterator kind" : "CONST",
                "value" : "income"
              },
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$income"
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
              "equality conditions" : {"id":5},
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