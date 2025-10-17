compiled-query-plan
{
"query file" : "idc_inner_join/q/q19.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "NESTED_LOOP_JOIN",
      "join predicates" : [
        { "outerBranch" :0, "outerExpr" : 0, "innerVar" : 0 }
      ],
      "branches" : [
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "company",
            "row variable" : "$$c",
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
          "FROM variable" : "$$c",
          "WHERE" : 
          {
            "iterator kind" : "OP_IS_NOT_NULL",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "head_office_location",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$c"
              }
            }
          },
          "SELECT expressions" : [
            {
              "field name" : "outerJoinVal1",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "company_id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$c"
                }
              }
            }
          ]
        },
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "company.null_records",
            "row variable" : "$$nr",
            "index used" : "primary index",
            "covering index" : false,
            "index scans" : [
              {
                "equality conditions" : {"company_id":0},
                "range conditions" : {}
              }
            ],
            "key bind expressions" : [
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$innerJoinVar0"
              }
            ],
            "map of key bind expressions" : [
              [ 0 ]
            ],
            "position in join" : 1
          },
          "FROM variable" : "$$nr",
          "WHERE" : 
          {
            "iterator kind" : "OP_IS_NULL",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "value",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$nr"
              }
            }
          },
          "SELECT expressions" : [

          ]
        }
      ]

    },
    "FROM variable" : "$join-0",
    "SELECT expressions" : [
      {
        "field name" : "company_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "company_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "head_office_location",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "head_office_location",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "record_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "record_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$nr"
          }
        }
      },
      {
        "field name" : "value",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "value",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$nr"
          }
        }
      }
    ]
  }
}
}
