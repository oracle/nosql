compiled-query-plan

{
"query file" : "rowprops/q/rsize02.q",
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
      "row variable" : "$f",
      "index used" : "idx_state_city_age",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.state":"MA"},
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
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "CONST",
                "value" : 170
              },
              "right operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f"
                }
              }
            },
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 185
              }
            }
          ]
        }
      }
    ]
  }
}
}