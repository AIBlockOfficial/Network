#!/bin/sh -v

echo " "
echo "//-----------------------------//"
echo "Deploy to nodes"
echo "//-----------------------------//"
echo " "
UPLOAD=$1
if [ "$2" = "stage_first" ]
then
    echo "Upload $UPLOAD to first only"
    UPLOAD_FIRST=1
    UPLOAD_OTHERS=0
elif [ "$2" = "stage_others" ]
then
    echo "Upload $UPLOAD to others only"
    UPLOAD_FIRST=0
    UPLOAD_OTHERS=1
elif [ "$2" = "skip_stage" ]
then
    echo "Not send $UPLOAD to any"
    UPLOAD_FIRST=0
    UPLOAD_OTHERS=0
else
    echo "Upload $UPLOAD to all"
    UPLOAD_FIRST=1
    UPLOAD_OTHERS=1
fi

if [ "$UPLOAD_FIRST" = "1" ]
then
    echo "Upload first"
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD  ubuntu@ec2-52-40-82-170.us-west-2.compute.amazonaws.com:~/
fi
if [ "$UPLOAD_OTHERS" = "1" ]
then
    echo "Upload Others"
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-52-27-248-13.us-west-2.compute.amazonaws.com:~/
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-34-219-102-194.us-west-2.compute.amazonaws.com:~/
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-34-221-188-21.us-west-2.compute.amazonaws.com:~/
fi
