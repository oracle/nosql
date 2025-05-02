compiled-query-plan

{
"query file" : "gb/q/distinct28.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-0",
  "input iterator" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 0, 1 ],
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
          "target table" : "Foo",
          "row variable" : "$$f",
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
            "field name" : "month",
            "field expression" : 
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : true,
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "month",
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
              ]
            }
          }
        ]
      }
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "acctno",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "month",
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