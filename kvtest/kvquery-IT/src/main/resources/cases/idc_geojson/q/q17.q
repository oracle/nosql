#operand1 is Point and operand2 is LineString, both geometries have a point in common

 select id, p.info.point
 from points p
 where geo_intersect(p.info.point,
                  {
        "type": "LineString",
        "coordinates": [
          [
            77.59847402572632,
            12.920574501916887
          ],
          [
            77.59848475456238,
            12.920516987352773
          ],
          [
            77.59843647480011,
            12.919900012104629
          ],
          [
            77.59840965270996,
            12.91891180281142
          ],
          [
            77.59838283061981,
            12.917426867210255
          ],
          [
            77.59833991527556,
            12.916904000460743
          ]
        ]
      })