compiled-query-plan

{
"query file" : "gb/q/distinct11.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "GROUP",
    "input variable" : "$gb-1",
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "distinct by fields at positions" : [ 1, 2, 3 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Bar",
          "row variable" : "$$f",
          "index used" : "idx_year_price",
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
            "iterator kind" : "ANY_GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "xact.items[].qty",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          },
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
          },
          {
            "field name" : "id1_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          },
          {
            "field name" : "id2_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          },
          {
            "field name" : "id3_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#id3",
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
}