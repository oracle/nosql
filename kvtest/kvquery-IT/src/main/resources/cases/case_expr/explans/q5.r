compiled-query-plan

{
"query file" : "case_expr/q/q5.q",
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
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "CASE",
          "clauses" : [
            {
              "when iterator" :
              {
                "iterator kind" : "GREATER_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "age",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 12
                }
              },
              "then iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "children",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            },
            {
              "when iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "city",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "address",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "Boston"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "age",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 11
                }
              }
            },
            {
              "when iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "city",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "address",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "Salem"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "lastname",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            },
            {
              "when iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "city",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "address",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f"
                    }
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : "San Fransisco"
                }
              },
              "then iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "age",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}