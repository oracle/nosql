compiled-query-plan

{
"query file" : "idc_uuid/q/q01.q",
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
      "distribution kind" : "ALL_PARTITIONS",
      "order by fields at positions" : [ 1 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "bar1",
          "row variable" : "$$f",
          "index used" : "primary index",
          "covering index" : true,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "firstName",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "firstName",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "uid1",
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
    "FROM variable" : "$from-1",
    "SELECT expressions" : [
      {
        "field name" : "firstName",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstName",
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
      "field name" : "firstName",
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