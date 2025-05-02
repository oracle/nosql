compiled-query-plan
{
"query file" : "idc_in_expr/q/q21.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "DELETE_ROW",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "SimpleDatatype.child",
          "row variable" : "$$p",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":0},
              "range conditions" : {}
            },
            {
              "equality conditions" : {"id":1},
              "range conditions" : {}
            }
          ]
        },
        "FROM variable" : "$$p",
        "WHERE" : 
        {
          "iterator kind" : "IN",
          "left-hand-side expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "age",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          ],
          "right-hand-side expressions" : [
            [
              {
                "iterator kind" : "CONST",
                "value" : 0
              },
              {
                "iterator kind" : "CONST",
                "value" : 28
              }
            ],
            [
              {
                "iterator kind" : "CONST",
                "value" : 1
              },
              {
                "iterator kind" : "CONST",
                "value" : 31
              }
            ]
          ]
        },
        "SELECT expressions" : [
          {
            "field name" : "id_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          {
            "field name" : "childId_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "childId",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$delcount-0",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "numRowsDeleted",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "numRowsDeleted",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$delcount-0"
          }
        }
      }
    }
  ]
}
}
