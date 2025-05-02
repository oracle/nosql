compiled-query-plan

{
"query file" : "upd/q/upd04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
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
                      "iterator kind" : "CONST",
                      "value" : 5
                    }
                  ]
                }
              ]
            }
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
          "field name" : "friends",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "George",
            "input iterator" :
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
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : ["Mark","John"]
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
          "field name" : "map2",
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
              "value" : "bar"
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
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 3
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
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "address",
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
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [

          ]
        }
      },
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "address",
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
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "areacode"
            },
            {
              "iterator kind" : "CONST",
              "value" : 400
            },
            {
              "iterator kind" : "CONST",
              "value" : "number"
            },
            {
              "iterator kind" : "CONST",
              "value" : 3445
            },
            {
              "iterator kind" : "CONST",
              "value" : "kind"
            },
            {
              "iterator kind" : "CONST",
              "value" : "work"
            }
          ]
        }
      },
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "address",
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
          }
        },
        "position iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 0
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "areacode"
            },
            {
              "iterator kind" : "CONST",
              "value" : 600
            },
            {
              "iterator kind" : "CONST",
              "value" : "number"
            },
            {
              "iterator kind" : "CONST",
              "value" : 13132
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
            "equality conditions" : {"id":3},
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
  }
}
}