compiled-query-plan

{
"query file" : "gb/q/noidx25.q",
"plan" : 
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
          "target table" : "numbers",
          "row variable" : "$$numbers",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "id" : { "end value" : 10, "end inclusive" : false } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$numbers",
        "SELECT expressions" : [
          {
            "field name" : "number",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "number",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$numbers"
              }
            }
          },
          {
            "field name" : "sum",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "decimal",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$numbers"
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "number",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "sum",
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
      "field name" : "number",
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
        "field name" : "sum",
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