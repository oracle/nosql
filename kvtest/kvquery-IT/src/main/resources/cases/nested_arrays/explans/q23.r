compiled-query-plan

{
"query file" : "nested_arrays/q/q23.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_age_areacode_kind",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.age":10},
          "range conditions" : { "info.addresses[].phones[][][].areacode" : { "start value" : 408, "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "age",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "ANY_GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "areacode",
              "input iterator" :
              {
                "iterator kind" : "ARRAY_FILTER",
                "predicate iterator" :
                {
                  "iterator kind" : "GREATER_OR_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "number",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 54
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
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
                        "field name" : "addresses",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$element"
                        }
                      }
                    }
                  }
                }
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 408
            }
          },
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
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 10
      }
    },
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
      }
    ]
  }
}
}