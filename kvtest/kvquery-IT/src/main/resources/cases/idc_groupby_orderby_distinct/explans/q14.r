compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q14.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
  "input iterator" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-2",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-1",
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
          "SELECT expressions" : [
            {
              "field name" : "Column_1",
              "field expression" : 
              {
                "iterator kind" : "IS_OF_TYPE",
                "target types" : [
                  {
                  "type" : "String",
                  "quantifier" : "",
                  "only" : false
                  }
                ],
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
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "Column_1",
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
          }
        ],
        "aggregate functions" : [

        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "Column_1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      },
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "bar2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    ],
    "aggregate functions" : [

    ]
  }
}
}