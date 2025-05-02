compiled-query-plan

{
"query file" : "idc_loj/q/q9.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
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
        "target table" : "A.B",
        "row variable" : "$$b",
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
      "FROM variables" : ["$$a", "$$b"],
      "SELECT expressions" : [
        {
          "field name" : "a_ida1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a"
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
        },
        {
          "field name" : "b_ida1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        {
          "field name" : "b3",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "b3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        }
      ]
    }
  }
}
}