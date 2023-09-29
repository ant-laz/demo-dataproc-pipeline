# demo-dataproc-pipeline

## 0 GCP Infra creation

### 0.0 check gcloud is on the right project

```sh
gcloud config configurations list
```

```sh
gcloud config configurations activate ${name_of_config}
```

### 0.1 Environmetal variables

To help build GCP infra, Create some evironmental variables as follows.

```sh
export GCP_PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")

export GCP_PROJECT_NUM=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectNumber)")

export GCP_REGION="europe-west2"

export DEMO_NAME=demo-dataproc-pipeline

export GCS_BUCKET=gs://${GCP_PROJECT_ID}-${DEMO_NAME}

export GCS_BUCKET_NO_PREFIX=${GCP_PROJECT_ID}-${DEMO_NAME}

export PUB_SUB_TOPIC=projects/${GCP_PROJECT_ID}/topics/${DEMO_NAME}

export DEBUG_PUB_SUB_SUBSCRIPTION=demodataprocpipelinedebug

export DEBUG_SUBSCRIPTION_ID=projects/${GCP_PROJECT_ID}/subscriptions/${DEBUG_PUB_SUB_SUBSCRIPTION}

export GCS_PUB_SUB_SUBSCRIPTION=demodataprocpipelinegcs

export GCS_SUBSCRIPTION_ID=projects/${GCP_PROJECT_ID}/subscriptions/${GCS_PUB_SUB_SUBSCRIPTION}

export EMAIL_ADDRESS=alazzaro@google.com

export TS_FORMAT=%Y-%m-%dT%H:%M:%SZ

export PUB_SUB_SA="service-${GCP_PROJECT_NUM}@gcp-sa-pubsub.iam.gserviceaccount.com"

export MYSQL_MVN_GROUP_ID="mysql"

export MYSQL_MVN_ARTIFACT="mysql-connector-java"

export MYSQL_MVN_VERSION="8.0.32"

export JDBC_JAR="gs://python-lab-329118-demo-dataproc-pipeline/mysql-connector-j-8.1.0.jar"

export CSQL_JAR="gs://python-lab-329118-demo-dataproc-pipeline/mysql-socket-factory-1.13.1.jar"

export DB_USER="anthony"

export DB_PASS="p#EQN65z\E(:,sv:"
```

### 0.2 enable GCP product apis

check which apis are currently enabled for your project by running

```sh
gcloud services list --enabled
```

If required, enable the dataproc, compute engine & cloud storage APIs

```sh
gcloud services enable dataproc.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage-component.googleapis.com
```


### 0.3 create pubsub topic

Our pipeline starts with a stream of messages in pub/sub so let's create a topic.

```sh
gcloud pubsub topics create ${PUB_SUB_TOPIC}
```

### 0.4 create gcs bucket

These pub/sub messages will be batched into files on GCS, so let's create a bucket.

```sh
gcloud storage buckets create ${GCS_BUCKET} \
  --project=${GCP_PROJECT_ID} \
  --location=${GCP_REGION} \
  --uniform-bucket-level-access
```

### 0.5 create pubsub debug subscription

Create a subscription to this topic for debugging purposes
```sh
gcloud pubsub subscriptions create ${DEBUG_PUB_SUB_SUBSCRIPTION} \
--topic=${PUB_SUB_TOPIC} \
--retain-acked-messages
```

### 0.6 pubsub cloud storage subscription

Create a Cloud Storage [subscription](https://cloud.google.com/pubsub/docs/create-cloudstorage-subscription#pubsub_create_cloudstorage_subscription-gcloud)
to consume pub/sub messages, batch them & write their contents as files on GCS.

As a prereq, the Pub/Sub service account must have permission to write to the specific 
Cloud Storage bucket and to read the bucket metadata.

```sh
gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET} \
--member="serviceAccount:${PUB_SUB_SA}" \
--role="roles/storage.objectCreator"

gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET} \
--member="serviceAccount:${PUB_SUB_SA}" \
--role="roles/storage.legacyBucketReader"
```

Disable the requester pays setting on the GCS bucket

```sh
gsutil requesterpays set off ${GCS_BUCKET}
```

Finally, execute this command to create the Cloud Storage Subscription.

```sh
gcloud pubsub subscriptions create ${GCS_PUB_SUB_SUBSCRIPTION} \
--topic=${PUB_SUB_TOPIC} \
--cloud-storage-bucket=${GCS_BUCKET_NO_PREFIX} \
--cloud-storage-file-prefix=shikharorder \
--cloud-storage-file-suffix=.jsonl \
--cloud-storage-max-duration=5m \
--cloud-storage-output-format=text \
--cloud-storage-write-metadata
```

### 0.7 dataproc cluster creation

```sh
gcloud dataproc clusters create ${DEMO_NAME} \
    --project=${GCP_PROJECT_ID} \
    --region=${GCP_REGION} \
    --single-node
```

### 0.8 authentication

Next create authentication details for your Google account

```sh
gcloud auth application-default login
```

### 0.9 cloudsql instance creation

https://cloud.google.com/sql/docs/mysql/create-instance#gcloud

```sh
gcloud sql instances create ${DEMO_NAME} \
--database-version=MYSQL_8_0 \
--cpu=2 \
--memory=7680MB \
--region=${GCP_REGION}
```

connect to this database

```sh
gcloud sql connect ${DEMO_NAME} --user=root --quiet
```

create a database

```sh
CREATE DATABASE guestbook;
```

create a table in this database & insert some data into the table

```sh
USE guestbook;
CREATE TABLE entries (guestName VARCHAR(255), content VARCHAR(255),
    entryID INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(entryID));
    INSERT INTO entries (guestName, content) values ("first guest", "I got here!");
INSERT INTO entries (guestName, content) values ("second guest", "Me too!");
```

check the newly created data is in the table

```sh
SELECT * FROM entries;
```


## 1 Cerating synthetic pub/sub messages for the simulation

This google provided template is used to create fake data.

https://cloud.google.com/dataflow/docs/guides/templates/provided/streaming-data-generator#gcloud

As we are generating pub/sub messages a JSON schema needs to be provided.

```js
{"id": {{integer(0,1000)}},"name": "{{uuid()}}","isInStock": {{bool()}}}
```

Note how the JSON is all one 1 line, this is so when we batch messages together in
a single file we can have a file in (jsonlines)[https://jsonlines.org/] format

Next upload our JSON schema into the GCS bucket

```sh
gcloud storage cp synth_data_schema.json ${GCS_BUCKET}
```
with the schema uploaded to gcs, a template can be kicked off later to generate data.

```sh
gcloud dataflow flex-template run demo-dataproc-gen-synth-pubsub-msgs \
    --project=${GCP_PROJECT_ID} \
    --region=${GCP_REGION} \
    --template-file-gcs-location=gs://dataflow-templates-${GCP_REGION}/latest/flex/Streaming_Data_Generator \
    --parameters \
sinkType=PUBSUB,\
topic=${PUB_SUB_TOPIC},\
schemaLocation=${GCS_BUCKET}/synth_data_schema.json,\
outputType=JSON,\
qps=5,\
messagesLimit=100
```

## Check pub/sub message creation

Check that pub/sub messages were create successfully.
N.B. This command will pull messages without acknowledgement.

```sh
gcloud pubsub subscriptions pull ${DEBUG_PUB_SUB_SUBSCRIPTION} \
--format=json \
--limit=10
```

## check bacthing of pub/sub messages & writing of files to GCS

```sh
gcloud storage ls ${GCS_BUCKET}
```

## Use cloud monitoring to measure the progress of the cloud storage subscription

https://cloud.google.com/pubsub/docs/monitoring#maintain_a_healthy_subscription

## Use pub/sub UI to measure progress of cloud storage subscription

https://cloud.google.com/pubsub/docs/monitor-subscription

## 3. Dataproc

### 3.1 PySpark to read from & write to files on GCS

```sh
gcloud dataproc clusters create ${DEMO_NAME}-1 \
    --project=${GCP_PROJECT_ID} \
    --region=${GCP_REGION} \
    --single-node
```

Using legacy RDD programming API

```sh
gcloud dataproc jobs submit pyspark rdd_read_text_from_gcs.py \
    --cluster=${DEMO_NAME}-1 \
    --region=${GCP_REGION} \
    -- gs://${GCS_BUCKET_NO_PREFIX}/input/ gs://${GCS_BUCKET_NO_PREFIX}/output/
```

Using newer Dataset API

```sh
gcloud dataproc jobs submit pyspark dataset_read_json_from_gcs.py \
    --cluster=${DEMO_NAME}-1 \
    --region=${GCP_REGION}
```

### 3.2 PySpark to read from & write to MySQL deployed on CloudSQL

Download the jdbc driver JAR 

https://dev.mysql.com/doc/connector-j/8.1/en/

https://mvnrepository.com/artifact/com.mysql/mysql-connector-j/8.1.0

Upload it to GCS

```sh
gcloud storage cp mysql-connector-j-8.1.0.jar ${GCS_BUCKET}
```

Download the Cloud SQL Java Connector JAR (for use with JDBC)

https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/blob/main/docs/jdbc-mysql.md

https://mvnrepository.com/artifact/com.google.cloud.sql/mysql-socket-factory/1.13.1

Upload it to GCS

```sh
gcloud storage cp mysql-socket-factory-1.13.1.jar ${GCS_BUCKET}
```

Build the full JCBD [URL](https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/blob/main/docs/jdbc-mysql.md#creating-thejdbc-url)

```
jdbc:mysql:///<DATABASE_NAME>?cloudSqlInstance=<INSTANCE_CONNECTION_NAME>&socketFactory=com.google.cloud.sql.mysql.SocketFactory&user=<MYSQL_USER_NAME>&password=<MYSQL_USER_PASSWORD>

```



Use it in the launch of a dataproc jobs

```sh
gcloud dataproc jobs submit pyspark dataset_read_from_cloudsql_mysql.py \
    --cluster=${DEMO_NAME}-2 \
    --region=${GCP_REGION} \
    --jars=${JDBC_JAR},${CSQL_JAR}
```

Use it in the launch of a dataproc jobs (ON LEON'S PROJECT)

```sh
gcloud dataproc jobs submit pyspark dataset_read_from_cloudsql_mysql.py \
    --cluster=dataproc-cloudsql-cluster \
    --region=europe-west2 \
    --jars=${JDBC_JAR},${CSQL_JAR}
```



https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/cloud-sql-proxy#using-this-initialization-action-without-configuring-hive-metastore

```sh
gcloud dataproc clusters create ${DEMO_NAME}-2 \
    --region ${GCP_REGION} \
    --scopes sql-admin \
    --initialization-actions gs://goog-dataproc-initialization-actions-${GCP_REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
    --metadata "enable-cloud-sql-hive-metastore=false" \
    --metadata "additional-cloud-sql-instances=${GCP_PROJECT_ID}:${GCP_REGION}:${DEMO_NAME}"
```



## 4 Cloud Composer

### 4.1 Cloud Composer - trigger every 5 mins

TODO

### 4.2 Cloud Composer - determine files on GCS to input based on current wall-clock-time

TODO

### 4.3 Cloud Composer - launch PySpark job with custom input

TODO

### 5 BigQuery

## 5 BigQuery  - monitoring jobs by inspecting jsonl files on GCS

setup a BigQuery BigLake table for the JSON files stored on GCS

https://cloud.google.com/bigquery/docs/create-cloud-storage-table-biglake

query this BigLake table to answer if a given YYYY/MM/DD/hh/mm/ combo had orders