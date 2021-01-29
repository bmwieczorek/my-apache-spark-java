#!/bin/bash
while getopts "e:h" OPT; do
    case $OPT in
        e  )
            env=$OPTARG
            echo "env=$env"
            if ! [[  -f "bin/$env/spark.properties" ]]
            then
                echo "Environment $env is not defined. Use -h for more details."
                exit 1
            fi
            ;;
        h  ) syntax
             exit 0 ;;
        ?  ) syntax
             exit 1 ;;
    esac
done
shift $(($OPTIND -1))

if [ "$env" == "" ]
then
  echo "Missing -e [dev|cert]"
  exit 1
fi

app=my-apache-spark-java-0.1-SNAPSHOT
mvn clean package -Pmake-dist -Denv=$env && \
scp target/$app-bin.tar.gz "$env"1: && \
ssh "$env"1 "rm -rf $app && tar xzf $app-bin.tar.gz && cd $app/bin && ./run.sh"
