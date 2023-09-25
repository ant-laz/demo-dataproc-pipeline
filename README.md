# demo-dataproc-pipeline

## GCP Infra creation

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
```

### pubsub topic

Our pipeline starts with a stream of messages in pub/sub so let's create a topic.

```sh
gcloud pubsub topics create ${PUB_SUB_TOPIC}
```

### gcs bucket

These pub/sub messages will be batched into files on GCS, so let's create a bucket.

```sh
gcloud storage buckets create ${GCS_BUCKET} \
  --project=${GCP_PROJECT_ID} \
  --location=${GCP_REGION} \
  --uniform-bucket-level-access
```

### pubsub debug subscription

Create a subscription to this topic for debugging purposes
```sh
gcloud pubsub subscriptions create ${DEBUG_PUB_SUB_SUBSCRIPTION} \
--topic=${PUB_SUB_TOPIC} \
--retain-acked-messages
```

### pubsub cloud storage subscription

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
--cloud-storage-file-prefix=demogcsprefix \
--cloud-storage-file-suffix=demogcssuffix \
--cloud-storage-max-bytes=10MB \
--cloud-storage-max-duration=1m \
--cloud-storage-output-format=text \
--cloud-storage-write-metadata
```

### authentication

Next create authentication details for your Google account

```sh
gcloud auth application-default login
```

## Cerating synthetic pub/sub messages for the simulation

This google provided template is used to create fake data.

https://cloud.google.com/dataflow/docs/guides/templates/provided/streaming-data-generator#gcloud

As we are generating pub/sub messages a JSON schema needs to be provided.

```js
{
  "id": {{integer(0,1000)}},
  "name": "{{uuid()}}",
  "isInStock": {{bool()}}
}
```
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
messagesLimit=10
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


## PySpark to read from & write to files on GCS

TODO

## PySpark to read from & write to MySQL deployed on CloudSQL

TODO

## Cloud Composer - trigger every 5 mins

TODO

## Cloud Composer - determine files on GCS to input based on current wall-clock-time

TODO

## Cloud Composer - launch PySpark job with custom input

TODO