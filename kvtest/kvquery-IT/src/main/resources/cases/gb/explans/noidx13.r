compiled-query-plan

{
"query file" : "gb/q/noidx13.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-2",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-1",
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
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "a",
            "field expression" : 
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "a",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mixed",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            }
          },
          {
            "field name" : "min",
            "field expression" : 
            {
              "iterator kind" : "FN_SEQ_MIN",
              "input iterator" :
              {
                "iterator kind" : "CASE",
                "clauses" : [
                  {
                    "when iterator" :
                    {
                      "iterator kind" : "IS_OF_TYPE",
                      "target types" : [
                        {
                        "type" : "Number",
                        "quantifier" : "",
                        "only" : false
                        }
                      ],
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "x",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "mixed",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$f"
                          }
                        }
                      }
                    },
                    "then iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "x",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "mixed",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f"
                        }
                      }
                    }
                  },
                  {
                    "else iterator" :
                    {
                      "iterator kind" : "SEQ_CONCAT",
                      "input iterators" : [

                      ]
                    }
                  }
                ]
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FN_MIN",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "min",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          }
        }
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "a",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-2"
      }
    }
  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FN_MIN",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "min",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    }
  ]
}
}