compiled-query-plan

{
"query file" : "gb/q/gb09.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "idx_long_bool",
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
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "long",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "long",
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
          }
        },
        {
          "field name" : "sum",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "int",
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
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "CONST",
        "value" : 2
      }
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "long",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "long",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "sum",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "sum",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ],
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  }
}
}