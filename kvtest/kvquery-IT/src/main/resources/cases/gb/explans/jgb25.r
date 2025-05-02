compiled-query-plan

{
"query file" : "gb/q/jgb25.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
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
          "index used" : "idx_store",
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
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "storeid",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "xact.storeid",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          },
          {
            "field name" : "Column_2",
            "field expression" : 
            {
              "iterator kind" : "FUNC_COUNT_STAR"
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-2",
    "GROUP BY" : "Grouping by the first expression in the SELECT list",
    "SELECT expressions" : [
      {
        "field name" : "storeid",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "storeid",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-2"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "Column_2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-2"
            }
          }
        }
      }
    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "storeid",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "storeid",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 1
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 3
  }
}
}