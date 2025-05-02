compiled-query-plan

{
"query file" : "joins_loj/q/lind20.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 4 ],
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "A",
          "row variable" : "$$a",
          "index used" : "a_idx_a1_a2_c1",
          "covering index" : true,
          "index row variable" : "$$a_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "descendant tables" : [
            { "table" : "A.B", "row variable" : "$$b", "covering primary index" : false },
            { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : false },
            { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : false }
          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a_idx", "$$b", "$$c", "$$d"],
        "SELECT expressions" : [
          {
            "field name" : "a",
            "field expression" : 
            {
              "iterator kind" : "RECORD_CONSTRUCTOR",
              "type" : { "Record" : {
                  "ida" : "Integer",
                  "a1" : "Integer",
                  "a2" : "Integer",
                  "c1" : "Integer"
                }
              },
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#ida",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$a_idx"
                  }
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "a1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$a_idx"
                  }
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "a2",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$a_idx"
                  }
                },
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "c1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$a_idx"
                  }
                }
              ]
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
            "field name" : "d",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$d"
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#ida",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a_idx"
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
    },
    {
      "field name" : "d",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "d",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
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
}