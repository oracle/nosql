compiled-query-plan
{
"query file" : "joins/q/lind26.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A",
        "row variable" : "$$a",
        "index used" : "primary index",
        "covering index" : true,
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
          "field name" : "ida",
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
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        {
          "field name" : "idb",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "CONST",
        "value" : 10
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "$from-0",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$from-0"
      }
    }
  ],
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 10
  }
}
}
