compiled-query-plan

{
"query file" : "gb/q/distinct10.q",
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
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"xact.year":2000},
              "range conditions" : { "xact.items[].qty" : { "start value" : 2, "start inclusive" : false } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "acctno",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "acctno",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "xact",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          },
          {
            "field name" : "id1_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          {
            "field name" : "id2_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          {
            "field name" : "id3_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          }
        ]
      }
    },
    "grouping expressions" : [
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "acctno",
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