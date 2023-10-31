.PHONY: build
build:
	docker buildx build -f Dockerfile -t foomo/dump-buckets:latest .