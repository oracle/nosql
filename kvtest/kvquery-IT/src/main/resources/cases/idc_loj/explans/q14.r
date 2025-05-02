compiled-query-plan

{
"query file" : "idc_loj/q/q14.q",
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
          "target table" : "A",
          "row variable" : "$$a",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "descendant tables" : [
            { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true }
          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a", "$$b"],
        "SELECT expressions" : [
          {
            "field name" : "a_a2",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "a2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a_a2",
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
      "field name" : "a_a2",
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