.PHONY: build
build:
	docker buildx build  --platform=linux/arm64/v8 -f Dockerfile -t foomo/dump-buckets:latest .

run:
	docker run --rm -it foomo/dump-buckets:latest dumpb