#!/bin/bash
SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR=$(cd $(dirname "$0"); pwd -P)

CURRENT_DATETIME=$(date +%Y%m%d_%H%M_%s)
APP_LOG_FILE=$SCRIPT_DIR/app-$CURRENT_DATETIME.log
YARN_LOG_FILE=$SCRIPT_DIR/app-$CURRENT_DATETIME.log

KRB_DOMAIN=$(grep -i default_realm /etc/krb5.conf | grep = | grep -Eo "[A-Za-z0-9_\.]+" | grep -vi default_realm | head -n 1)
KRB_KEYTAB=$USER.keytab
KRB_PRINCIPAL=$USER@$KRB_DOMAIN

#kinit
#KRB_MASTER_KDC_HOSTNAME=$(grep -i master_kdc /etc/krb5.conf | grep = | grep -Eo "[A-Za-z0-9_]+" | grep -vi master_kdc | head -n 1)
#KRB_MASTER_KDC_DOMAIN=$(grep -i domain /etc/krb5.conf | grep = | grep -Eo "[A-Za-z0-9_\.]+" | grep -vi domain | head -n 1)
#ipa-getkeytab -s $KRB_MASTER_KDC_HOSTNAME.$KRB_MASTER_KDC_DOMAIN -p $KRB_PRINCIPAL -P -k $HOME/security/$KRB_KEYTAB
kinit -kt $HOME/security/$KRB_KEYTAB $KRB_PRINCIPAL

spark2-submit --master yarn \
              --deploy-mode cluster \
              --name "MY APACHE SPARK JAVA" \
              --class "com.bawi.spark.SparkApp" \
              --files "$SCRIPT_DIR/app.properties,$SCRIPT_DIR/metrics.properties,$SCRIPT_DIR/log4j.properties#log4j.properties,$HOME/security/$KRB_KEYTAB#$KRB_KEYTAB,$HOME/security/jaas.conf#jaas.conf" \
              --driver-java-options="-Djava.security.auth.login.config=./jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
              --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
              --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
              --conf "spark.metrics.conf=metrics.properties" \
              --conf spark.sql.catalogImplementation=hive \
              --properties-file "$SCRIPT_DIR/spark.properties" \
              "$SCRIPT_DIR/../lib/my-apache-spark-java-@project.version@.jar" \
              --job-properties "app.properties" \
               2>&1 | tee -a "$APP_LOG_FILE"


applicationId=$(grep "tracking URL" "$APP_LOG_FILE" | head -n 1 | grep -Eo "application_[0-9_]+")
yarn logs -applicationId "$applicationId" 2>&1 | tee -a "YARN_LOG_FILE"
