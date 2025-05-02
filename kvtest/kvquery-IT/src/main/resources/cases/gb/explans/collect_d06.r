compiled-query-plan

{
"query file" : "gb/q/collect_d06.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2, 0 ],
  "input iterator" :
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
          "GROUP BY" : "Grouping by the first expression in the SELECT list",
          "SELECT expressions" : [
            {
              "field name" : "gb-0",
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
              "field name" : "aggr-1",
              "field expression" : 
              {
                "iterator kind" : "FUNC_COLLECT",
                "distinct" : true,
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "xact.prodcat",
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
      "FROM variable" : "$from-1",
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "gb-0",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        },
        {
          "field name" : "aggr-1",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COLLECT",
            "distinct" : true,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$from-1"
              }
            }
          }
        }
      ]
    },
    "FROM variable" : "$from-0",
    "SELECT expressions" : [
      {
        "field name" : "acctno",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      },
      {
        "field name" : "prodcats",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "aggr-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-0"
          }
        }
      },
      {
        "field name" : "cnt",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SIZE",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-0"
            }
          }
        }
      }
    ]
  }
}
}