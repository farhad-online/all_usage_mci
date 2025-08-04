JAR_PATH="../../../jars"
KEYTAB_PATH="../../../keytabs"
CONFIG_PATH="../../../config"
JOB_PATH="."
JOB_NAME="all_usage_network_switch"

sudo -u spark spark-submit \
        --deploy-mode client \
        --name $JOB_NAME \
        --jars ${JAR_PATH}/config-1.4.1.jar,./jars/hive-serde-3.1.3.jar,./jars/com.github.luben_zstd-jni-1.4.8-1.jar,./jars/org.apache.commons_commons-pool2-2.6.2.jar,./jars/org.apache.kafka_kafka-clients-2.6.0.jar,./jars/org.apache.spark_spark-sql-kafka-0-10_2.12-3.1.3.jar,./jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.1.3.jar,./jars/org.lz4_lz4-java-1.7.1.jar,./jars/org.slf4j_slf4j-api-1.7.30.jar,./jars/org.spark-project.spark_unused-1.0.0.jar,./jars/org.xerial.snappy_snappy-java-1.1.8.2.jar \
        --files ${KEYTAB_PATH}/kafka.service.keytab,${CONFIG_PATH}/spark_jaas.conf,${JOB_PATH}/${JOB_NAME}.conf \
        --conf spark.yarn.submit.waitAppCompletion=false \
        --conf spark.security.credentials.hbase.enabled=false \
        --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=${CONFIG_PATH}/spark_jaas.conf" \
        --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=${CONFIG_PATH}/spark_jaas.conf" \
        --conf spark.yarn.principal=yarn/master.dwbi.mci@DWBI.MCI \
        --conf spark.yarn.keytab=${KEYTAB_PATH}/yarn.service.keytab \
        --keytab ${KEYTAB_PATH}/spark.service.keytab \
        --principal spark/master.dwbi.mci@DWBI.MCI \
        --conf spark.yarn.queue=default \
        ${JOB_NAME}.jar $JOB_NAME ${JOB_PATH}/${JOB_NAME}.conf