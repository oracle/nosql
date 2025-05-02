compiled-query-plan

{
"query file" : "case_expr/q/q9.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_state_city_age",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
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
                  "field name" : "address.city",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
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
                "iterator kind" : "FIELD_STEP",
                "field name" : "#id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              }
            },
            {
              "else iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "age",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
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