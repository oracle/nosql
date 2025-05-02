compiled-query-plan

{
"query file" : "case_expr/q/q7.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$f",
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
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [
            {
              "iterator kind" : "CASE",
              "clauses" : [
                {
                  "when iterator" :
                  {
                    "iterator kind" : "EQUAL",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "state",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "address",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$f"
                        }
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : "MA"
                    }
                  },
                  "then iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "work",
                    "input iterator" :
                    {
                      "iterator kind" : "ARRAY_FILTER",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "phones",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "address",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$f"
                          }
                        }
                      }
                    }
                  }
                },
                {
                  "else iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "home",
                    "input iterator" :
                    {
                      "iterator kind" : "ARRAY_FILTER",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "phones",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "address",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$f"
                          }
                        }
                      }
                    }
                  }
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
}