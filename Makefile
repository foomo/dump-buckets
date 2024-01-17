.PHONY: build
build:
	docker buildx build -f Dockerfile -t dump-buckets:latest .

run: build
	docker run --rm -it \
		-e BACKUP_NAME=execute-example \
    	-e STORAGE_VENDOR=gcs \
    	-e STORAGE_BUCKET_NAME=dumpb-test-bucket \
    	-e STORAGE_PATH=execute-example \
		-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcloud/application_default_credentials.json \
		-v $(HOME)/.config/gcloud/application_default_credentials.json:/tmp/gcloud/application_default_credentials.json:ro \
	dump-buckets:latest execute -- echo "hello"