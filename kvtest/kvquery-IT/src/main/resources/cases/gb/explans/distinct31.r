compiled-query-plan

{
"query file" : "gb/q/distinct31.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
    "input iterator" :
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
            "target table" : "fooNew",
            "row variable" : "$$f",
            "index used" : "idx_barNew1234",
            "covering index" : true,
            "index row variable" : "$$f_idx",
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : { "info.bar1" : { "end value" : 7, "end inclusive" : true } }
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$f_idx",
          "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
          "SELECT expressions" : [
            {
              "field name" : "bar1",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              }
            },
            {
              "field name" : "bar2",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              }
            },
            {
              "field name" : "Column_3",
              "field expression" : 
              {
                "iterator kind" : "FN_COUNT",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info.bar1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                }
              }
            }
          ]
        }
      },
      "FROM variable" : "$from-2",
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "bar1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "bar1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-2"
            }
          }
        },
        {
          "field name" : "bar2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "bar2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-2"
            }
          }
        },
        {
          "field name" : "Column_3",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "Column_3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$from-2"
              }
            }
          }
        }
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "bar1",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "bar2",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "Column_3",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}