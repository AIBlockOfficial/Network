#!/bin/sh -v

echo " "
echo "//-----------------------------//"
echo "Deploy to nodes"
echo "//-----------------------------//"
echo " "
UPLOAD=$1
if [ "$2" = "stage_first" ]
    echo "Upload $UPLOAD to first only"
    UPLOAD_OTHERS=0
then
    echo "Upload $UPLOAD to all"
    UPLOAD_OTHERS=1
fi

echo "Upload first"
scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD  ubuntu@ec2-52-40-82-170.us-west-2.compute.amazonaws.com:~/

if [ "$UPLOAD_OTHERS" = "1" ]
then
    echo "Upload Others"
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-52-27-248-13.us-west-2.compute.amazonaws.com:~/
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-34-219-102-194.us-west-2.compute.amazonaws.com:~/
    scp -i ~/.ssh/Zenotta-Node.pem $UPLOAD ubuntu@ec2-34-221-188-21.us-west-2.compute.amazonaws.com:~/
fi
