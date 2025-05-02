compiled-query-plan

{
"query file" : "gb/q/distinct24.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
        "index used" : "idx_acc_year_prodcat",
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
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "acctno",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "xact.acctno",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "year",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "xact.year",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "acctno",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "acctno",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "year",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "year",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    }
  ]
}
}