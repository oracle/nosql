compiled-query-plan

{
"query file" : "joins/q/lind23.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
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
            { "table" : "A.B", "row variable" : "$$b", "covering primary index" : false }
          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a", "$$b"],
        "SELECT expressions" : [
          {
            "field name" : "a",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a"
            }
          },
          {
            "field name" : "b",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "a",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "a",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "b",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "b",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}