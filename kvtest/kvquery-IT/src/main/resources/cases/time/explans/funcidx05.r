compiled-query-plan

{
"query file" : "time/q/funcidx05.q",
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
        "target table" : "bar",
        "row variable" : "$$bar",
        "index used" : "idx_year_hour",
        "covering index" : true,
        "index row variable" : "$$bar_idx",
        "index scans" : [
          {
            "equality conditions" : {"year#tm":2021},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$bar_idx",
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "Column_1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "hour#tm",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$bar_idx"
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}