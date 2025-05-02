compiled-query-plan

{
"query file" : "gb/q/distinct22.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-3",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "GROUP",
        "input variable" : "$gb-2",
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
          "SELECT expressions" : [
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
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "year",
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
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "year",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-3"
        }
      }
    ],
    "aggregate functions" : [

    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "year",
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