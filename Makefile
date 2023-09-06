.PHONY: build
build:
	docker build -f Dockerfile -t foomo/dump-buckets:latest .