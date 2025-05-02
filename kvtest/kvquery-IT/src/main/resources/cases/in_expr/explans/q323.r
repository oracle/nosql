compiled-query-plan

{
"query file" : "in_expr/q/q323.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "IN",
            "left-hand-side expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              }
            ],
            "right-hand-side expressions" : [
              {
                "iterator kind" : "ARRAY_FILTER",
                "input iterator" :
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$arr17"
                }
              }
            ]
          },
          {
            "iterator kind" : "IN",
            "left-hand-side expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info.bar4",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              }
            ],
            "right-hand-side expressions" : [
              {
                "iterator kind" : "ARRAY_FILTER",
                "input iterator" :
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$arr19"
                }
              }
            ]
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
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
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}