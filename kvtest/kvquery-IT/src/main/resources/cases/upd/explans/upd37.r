compiled-query-plan

{
"query file" : "upd/q/upd37.q",
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
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "MAP_CONSTRUCTOR",
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : "areacode"
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 650
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : "number"
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 400
                    }
                  ]
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
                      "value" : 650
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : "number"
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 400
                    }
                  ]
                }
              ]
            }
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
              "equality conditions" : {"id":30},
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