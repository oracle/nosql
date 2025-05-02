compiled-query-plan

{
"query file" : "idc_geojson/q/q79.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
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
        "target table" : "points",
        "row variable" : "$$p",
        "index used" : "idx_ptn",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9v1", "start inclusive" : true, "end value" : "t9v1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9v3", "start inclusive" : true, "end value" : "t9v7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9v9", "start inclusive" : true, "end value" : "t9v9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9vc", "start inclusive" : true, "end value" : "t9vzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9y1", "start inclusive" : true, "end value" : "t9y1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9y3", "start inclusive" : true, "end value" : "t9y7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9y9", "start inclusive" : true, "end value" : "t9y9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9yc", "start inclusive" : true, "end value" : "t9yzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9z1", "start inclusive" : true, "end value" : "t9z1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9z3", "start inclusive" : true, "end value" : "t9z7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9z9", "start inclusive" : true, "end value" : "t9z9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9zc", "start inclusive" : true, "end value" : "t9zzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcb1", "start inclusive" : true, "end value" : "tcb1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcb3", "start inclusive" : true, "end value" : "tcb7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcb9", "start inclusive" : true, "end value" : "tcb9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcbc", "start inclusive" : true, "end value" : "tcbzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcc1", "start inclusive" : true, "end value" : "tcc1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcc4", "start inclusive" : true, "end value" : "tcc5zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcch", "start inclusive" : true, "end value" : "tccjzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tccn", "start inclusive" : true, "end value" : "tccpzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdj0", "start inclusive" : true, "end value" : "tdjzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdm0", "start inclusive" : true, "end value" : "tdrzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdt0", "start inclusive" : true, "end value" : "tdtzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdv0", "start inclusive" : true, "end value" : "tdv3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdv8", "start inclusive" : true, "end value" : "tdvczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdw0", "start inclusive" : true, "end value" : "tdy3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdy8", "start inclusive" : true, "end value" : "tdyczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdz0", "start inclusive" : true, "end value" : "tdz3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdz8", "start inclusive" : true, "end value" : "tdzczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf00", "start inclusive" : true, "end value" : "tf11zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf14", "start inclusive" : true, "end value" : "tf15zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf1h", "start inclusive" : true, "end value" : "tf1jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf1n", "start inclusive" : true, "end value" : "tf1pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf20", "start inclusive" : true, "end value" : "tf31zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf34", "start inclusive" : true, "end value" : "tf35zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf3h", "start inclusive" : true, "end value" : "tf3jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf3n", "start inclusive" : true, "end value" : "tf3pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf80", "start inclusive" : true, "end value" : "tf91zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf94", "start inclusive" : true, "end value" : "tf95zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf9h", "start inclusive" : true, "end value" : "tf9jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf9n", "start inclusive" : true, "end value" : "tf9pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfb0", "start inclusive" : true, "end value" : "tfb3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfb8", "start inclusive" : true, "end value" : "tfbczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfc0", "start inclusive" : true, "end value" : "tfc1zzzzzz", "end inclusive" : true } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$p",
      "WHERE" : 
      {
        "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
        "search target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "point",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$p"
            }
          }
        },
        "search geometry iterator" :
        {
          "iterator kind" : "CONST",
          "value" : {"coordinates":[[[[77.50614166259766,12.966754416691947],[77.60295867919922,12.947348706524428],[77.61051177978516,12.908198108318507],[77.50511169433594,12.888787845039232],[77.55146026611328,12.879416834404825],[77.59712219238281,12.875735270042483],[77.65617370605469,12.901170428290447],[77.70492553710938,12.963743284940753],[77.67539978027344,13.033659039547265],[77.52056121826172,12.999874459369373],[77.50614166259766,12.966754416691947]]],[[[77.55935668945312,13.057740282292977],[77.52948760986328,13.065766841807562],[77.45155334472656,13.048375633247892],[77.4481201171875,13.016600074665284],[77.43438720703125,12.94701411205675],[77.46047973632811,12.962739566261234],[77.49000549316406,12.967758119178917],[77.50991821289062,12.982144076424275],[77.49378204345703,13.004892262383622],[77.52124786376953,13.032655603554861],[77.55935668945312,13.056402497014313],[77.52777099609375,13.002550633604786],[77.49412536621094,13.00355419150191],[77.55935668945312,13.057740282292977]]]],"type":"MultiPolygon"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 300000.098
        }
      },
      "SELECT expressions" : [
        {
          "field name" : "id",
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
          "field name" : "dist",
          "field expression" : 
          {
            "iterator kind" : "GEO_DISTANCE",
            "first geometry iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "point",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              }
            },
            "second geometry iterator" :
            {
              "iterator kind" : "CONST",
              "value" : {"coordinates":[[[[77.50614166259766,12.966754416691947],[77.60295867919922,12.947348706524428],[77.61051177978516,12.908198108318507],[77.50511169433594,12.888787845039232],[77.55146026611328,12.879416834404825],[77.59712219238281,12.875735270042483],[77.65617370605469,12.901170428290447],[77.70492553710938,12.963743284940753],[77.67539978027344,13.033659039547265],[77.52056121826172,12.999874459369373],[77.50614166259766,12.966754416691947]]],[[[77.55935668945312,13.057740282292977],[77.52948760986328,13.065766841807562],[77.45155334472656,13.048375633247892],[77.4481201171875,13.016600074665284],[77.43438720703125,12.94701411205675],[77.46047973632811,12.962739566261234],[77.49000549316406,12.967758119178917],[77.50991821289062,12.982144076424275],[77.49378204345703,13.004892262383622],[77.52124786376953,13.032655603554861],[77.55935668945312,13.056402497014313],[77.52777099609375,13.002550633604786],[77.49412536621094,13.00355419150191],[77.55935668945312,13.057740282292977]]]],"type":"MultiPolygon"}
            }
          }
        }
      ]
    }
  }
}
}