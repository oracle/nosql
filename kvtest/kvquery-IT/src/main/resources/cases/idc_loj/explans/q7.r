compiled-query-plan

{
"query file" : "idc_loj/q/q7.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 3, 5 ],
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
            { "table" : "A", "row variable" : "$$a", "covering primary index" : false }          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a", "$$b"],
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
            "field name" : "idb1",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idb1",
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
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idb2",
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
  },
  "FROM variable" : "$from-0",
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
          "variable" : "$from-0"
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
          "variable" : "$from-0"
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
          "variable" : "$from-0"
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
          "variable" : "$from-0"
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
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}