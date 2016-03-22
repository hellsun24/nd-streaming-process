#!/bin/bash

message="6000    2016-03-12     6003    9025871129044750487     45bac650-e80f-11e5-849a-44a842334e4d    1       1161392705000825979     11747   18347   2.5352525252525253      18316   3F5EBABA563CE51B132B41553571E3BC        0       0       0       0       0"

while true
do 
    echo $message >> /var/log/kafkatest2;
done
