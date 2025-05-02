compiled-query-plan

{
"query file" : "prim_index/q/in02.q",
"plan" : 
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
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id1":3},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"id1":4},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "IN",
        "left-hand-side expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$foo"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$foo"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id4",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$foo"
            }
          }
        ],
        "right-hand-side expressions" : [
          [
            {
              "iterator kind" : "CONST",
              "value" : 3
            },
            {
              "iterator kind" : "CONST",
              "value" : "tok0"
            },
            {
              "iterator kind" : "CONST",
              "value" : "id4-3"
            }
          ],
          [
            {
              "iterator kind" : "CONST",
              "value" : 4
            },
            {
              "iterator kind" : "CONST",
              "value" : "tok1"
            },
            {
              "iterator kind" : "CONST",
              "value" : "id4-4"
            }
          ]
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      {
        "field name" : "id2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      {
        "field name" : "id3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      {
        "field name" : "id4",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      }
    ]
  }
}
}