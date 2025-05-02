compiled-query-plan

{
"query file" : "idc_loj/q/q8.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1, 2 ],
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
          { "table" : "A", "row variable" : "$$a", "covering primary index" : false }        ],
        "position in join" : 0
      },
      "FROM variables" : ["$$a", "$$c"],
      "SELECT expressions" : [
        {
          "field name" : "ida1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "idb1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "idc1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idc1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "idc2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idc2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "c3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "a2",
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
        },
        {
          "field name" : "a3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a3",
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
}
}