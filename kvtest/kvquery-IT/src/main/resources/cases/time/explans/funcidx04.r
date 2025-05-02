compiled-query-plan

{
"query file" : "time/q/funcidx04.q",
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
      "target table" : "bar",
      "row variable" : "$$bar",
      "index used" : "idx_year_month",
      "covering index" : true,
      "index row variable" : "$$bar_idx",
      "index scans" : [
        {
          "equality conditions" : {"year#tm":2021,"month#tm":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$bar_idx"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "STRING_CONCAT",
          "input iterators" : [
            {
              "iterator kind" : "CAST",
              "target type" : "String",
              "quantifier" : "*",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "year#tm",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$bar_idx"
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "-"
            },
            {
              "iterator kind" : "CAST",
              "target type" : "String",
              "quantifier" : "*",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "month#tm",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$bar_idx"
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