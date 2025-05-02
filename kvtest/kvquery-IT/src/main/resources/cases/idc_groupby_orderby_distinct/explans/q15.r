compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q15.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-2",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
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
            "target table" : "ComplexType",
            "row variable" : "$$p",
            "index used" : "primary index",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : { "id" : { "start value" : 0, "start inclusive" : true } }
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$p",
          "SELECT expressions" : [
            {
              "field name" : "flt",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "flt",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              }
            },
            {
              "field name" : "Column_2",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              }
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "flt",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          }
        ],
        "aggregate functions" : [
          {
            "iterator kind" : "FN_COUNT",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "Column_2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$gb-1"
              }
            }
          }
        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "flt",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    ],
    "aggregate functions" : [
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Column_2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-2"
          }
        }
      }
    ]
  }
}
}