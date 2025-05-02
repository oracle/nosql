compiled-query-plan

{
"query file" : "idc_schemaless/q/q26.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-0",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "order by fields at positions" : [ 1 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "jsoncol",
          "row variable" : "$$jc",
          "index used" : "idx_index",
          "covering index" : true,
          "index row variable" : "$$jc_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$jc_idx",
        "SELECT expressions" : [
          {
            "field name" : "minorKey",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#minorKey",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$jc_idx"
              }
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "index",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$jc_idx"
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-1",
    "SELECT expressions" : [
      {
        "field name" : "minorKey",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "minorKey",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "minorKey",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}