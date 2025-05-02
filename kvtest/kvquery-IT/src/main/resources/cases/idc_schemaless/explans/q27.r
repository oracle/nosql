compiled-query-plan

{
"query file" : "idc_schemaless/q/q27.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-0",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "SORT",
      "order by fields at positions" : [ 1 ],
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "ALL_PARTITIONS",
        "input iterator" :
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "jsoncol",
            "row variable" : "$$jsoncol",
            "index used" : "primary index",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : {}
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$jsoncol",
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
                  "variable" : "$$jsoncol"
                }
              }
            },
            {
              "field name" : "sort_gen",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "firstThread",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$jsoncol"
                }
              }
            }
          ]
        }
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