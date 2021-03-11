#!/bin/sh

echo " "
echo "//-----------------------------//"
echo "Deploy to nodes"
echo "//-----------------------------//"
echo " "

UPLOAD=$2
BASE_NAME_UPLOAD=`basename $UPLOAD`

NODE_ADDR_1=ubuntu@ec2-52-40-82-170.us-west-2.compute.amazonaws.com
NODE_ADDR_2=ubuntu@ec2-52-27-248-13.us-west-2.compute.amazonaws.com
NODE_ADDR_3=ubuntu@ec2-44-239-251-56.us-west-2.compute.amazonaws.com
NODE_ADDR_4=ubuntu@ec2-35-155-18-122.us-west-2.compute.amazonaws.com

if [ "$1" = "stage_first" ]
then
    echo "Upload $UPLOAD to first only"
    UPLOAD_FIRST=1
    UPLOAD_OTHERS=0
elif [ "$1" = "stage_others" ]
then
    echo "Upload $UPLOAD to others only"
    UPLOAD_FIRST=0
    UPLOAD_OTHERS=1
elif [ "$1" = "stage_all" ]
then
    echo "Upload $UPLOAD to all"
    UPLOAD_FIRST=1
    UPLOAD_OTHERS=1
else
    echo "Not send $UPLOAD to any: $1"
    UPLOAD_FIRST=0
    UPLOAD_OTHERS=0
fi

if [ "$3" = "complete_deploy_first" ]
then
    echo "Complete deploy first"
    COMPLETE_DEPLOY_FIRST=1
    COMPLETE_DEPLOY_OTHERS=0
elif [ "$3" = "complete_deploy_others" ]
then
    echo "Complete deploy others"
    COMPLETE_DEPLOY_FIRST=0
    COMPLETE_DEPLOY_OTHERS=1
elif [ "$3" = "complete_deploy_all" ]
then
    echo "Complete deploy all"
    COMPLETE_DEPLOY_FIRST=1
    COMPLETE_DEPLOY_OTHERS=1
else
    echo "Not complete deploy any: $3"
    COMPLETE_DEPLOY_FIRST=0
    COMPLETE_DEPLOY_OTHERS=0
fi

if [ "$4" = "check_connection" ]
then
    echo "Check connection first"
    CHECK_CONNECTION=1
else
    echo "Not check connection first: $4"
    CHECK_CONNECTION=0
fi

if [ "$5" = "get_existing_logs" ]
then
    echo "Get existing logs"
    GET_EXISTING_LOGS=1
else
    echo "Not get existing logs: $5"
    GET_EXISTING_LOGS=0
fi

if [ "$6" = "start_with_clean_db" ]
then
    echo "Will start node by wiping all databases"
    START_WITH_CLEAN="start_with_clean_db"
elif [ "$6" = "start_with_wipe_znp" ]
then
    echo "Will start node by wiping all znp folder"
    START_WITH_CLEAN="start_with_wipe_znp"
else
    echo "Will start node with existing znp folder/databases"
    START_WITH_CLEAN="start_with_existing"
fi

if [ "$7" != "no_prompt" ]
then
    read -p "Continue (y/n)?" CONT
    if [ "$CONT" = "y" ]
    then
        echo "Proceeding"
    else
        echo "Quiting"
        exit
    fi
fi

echo " "
echo "//-----------------------------//"
echo "Check nodes connection"
echo "//-----------------------------//"
echo " "
if [ "$CHECK_CONNECTION" = "1" ]
then
    set -x
    ssh -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_1 echo test_connection
    ssh -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_2 echo test_connection
    ssh -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_3 echo test_connection
    ssh -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_4 echo test_connection
    set +x
fi

echo " "
echo "//-----------------------------//"
echo "Get existing logs"
echo "//-----------------------------//"
echo " "
if [ "$GET_EXISTING_LOGS" = "1" ]
then
    set -x
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_1:~/znp/storage_0.log ./
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_2:~/znp/miner_0.log ./
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_3:~/znp/compute_0.log ./
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_4:~/znp/user_0.log ./
    set +x
fi

echo " "
echo "//-----------------------------//"
echo "Stage heavy tar.gz file"
echo "//-----------------------------//"
echo " "

if [ "$UPLOAD_FIRST" = "1" ]
then
    echo "Upload first"
    set -v
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $UPLOAD $NODE_ADDR_1:~/
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem src/bin/node_settings_aws_deploy.sh $NODE_ADDR_1:~/
    set +v
fi
if [ "$UPLOAD_OTHERS" = "1" ]
then
    echo "Upload Others"
    set -v
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $UPLOAD $NODE_ADDR_2:~/
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $UPLOAD $NODE_ADDR_3:~/
    scp -o ConnectTimeout=5 -i ~/.ssh/Zenotta-Node.pem $UPLOAD $NODE_ADDR_4:~/
    set +v
fi

echo " "
echo "//-----------------------------//"
echo "Create deploy scripts"
echo "//-----------------------------//"
echo " "
set -v
cat src/bin/node_settings_aws_run_one.sh | sed -e "s/\$1/storage/g" | sed -e "s/\$2/0/g" | sed -e "s/\$3/0/g" | sed -e "s/\$4/info/g" | sed -e "s/\$5/$BASE_NAME_UPLOAD/g" | sed -e "s/\$6/$START_WITH_CLEAN/g" > target/release/node_settings_aws_run_storage_0.sh
cat src/bin/node_settings_aws_run_one.sh | sed -e "s/\$1/compute/g" | sed -e "s/\$2/0/g" | sed -e "s/\$3/0/g" | sed -e "s/\$4/info/g" | sed -e "s/\$5/$BASE_NAME_UPLOAD/g" | sed -e "s/\$6/$START_WITH_CLEAN/g" > target/release/node_settings_aws_run_compute_0.sh
cat src/bin/node_settings_aws_run_one.sh | sed -e "s/\$1/miner/g" | sed -e "s/\$2/0/g" | sed -e "s/\$3/0/g" | sed -e "s/\$4/info/g" | sed -e "s/\$5/$BASE_NAME_UPLOAD/g" | sed -e "s/\$6/$START_WITH_CLEAN/g" > target/release/node_settings_aws_run_miner_0.sh
cat src/bin/node_settings_aws_run_one.sh | sed -e "s/\$1/user/g" | sed -e "s/\$2/0/g" | sed -e "s/\$3/0/g" | sed -e "s/\$4/info/g" | sed -e "s/\$5/$BASE_NAME_UPLOAD/g" | sed -e "s/\$6/$START_WITH_CLEAN/g" > target/release/node_settings_aws_run_user_0.sh
set +v

if [ "$COMPLETE_DEPLOY_FIRST" = "1" ]
then
    echo "Complete deploy first"
    set -x
    scp -i ~/.ssh/Zenotta-Node.pem target/release/node_settings_aws_run_storage_0.sh $NODE_ADDR_1:~/
    ssh  -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_1 screen -S auto_deploy -d -m -L -Logfile auto_deploy_screen.log sh ./node_settings_aws_run_storage_0.sh
    set +x
fi
if [ "$COMPLETE_DEPLOY_OTHERS" = "1" ]
then
    echo "Complete deploy Others"
    set -x
    scp -i ~/.ssh/Zenotta-Node.pem target/release/node_settings_aws_run_miner_0.sh $NODE_ADDR_2:~/
    scp -i ~/.ssh/Zenotta-Node.pem target/release/node_settings_aws_run_compute_0.sh $NODE_ADDR_3:~/
    scp -i ~/.ssh/Zenotta-Node.pem target/release/node_settings_aws_run_user_0.sh $NODE_ADDR_4:~/

    ssh  -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_2 screen -S auto_deploy -d -m -L -Logfile auto_deploy_screen.log sh ./node_settings_aws_run_miner_0.sh
    ssh  -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_3 screen -S auto_deploy -d -m -L -Logfile auto_deploy_screen.log sh ./node_settings_aws_run_compute_0.sh
    ssh  -i ~/.ssh/Zenotta-Node.pem $NODE_ADDR_4 screen -S auto_deploy -d -m -L -Logfile auto_deploy_screen.log sh ./node_settings_aws_run_user_0.sh

    set +x
fi

set +x
set +v
