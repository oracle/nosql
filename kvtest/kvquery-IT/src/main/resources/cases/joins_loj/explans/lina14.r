compiled-query-plan

{
"query file" : "joins_loj/q/lina14.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 3 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A.B.C",
        "row variable" : "$$c",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "ancestor tables" : [
          { "table" : "A", "row variable" : "$$a", "covering primary index" : false },
          { "table" : "A.B", "row variable" : "$$b", "covering primary index" : false }        ],
        "position in join" : 0
      },
      "FROM variables" : ["$$a", "$$b", "$$c"],
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
          "field name" : "c",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
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
              "variable" : "$$c"
            }
          }
        }
      ]
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
    },
    {
      "field name" : "c",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "c",
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