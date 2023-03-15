IMAGE_NAME = batch-materialize-env
LOCAL_USER_HOME = $(shell printf ~)
LOCAL_AWS_FOLDER = $(LOCAL_USER_HOME)/.aws
DOCKER_AWS_FOLDER = /root/.aws

.PHONY: build-image-nc
build-image-nc:
	docker build --no-cache -t $(IMAGE_NAME) .

.PHONY: run-local-docker
run-local-docker:
	docker run \
	-v $(LOCAL_AWS_FOLDER):$(DOCKER_AWS_FOLDER) \
	-e AWS_PROFILE=$(AWS_PROFILE) \
	$(IMAGE_NAME) python3 materialize_athena_query.py some_project/sample-nyc-covid.sql <<<TARGET_BUCKET>>> <<<TAGET_DB>>> <<<TARGET_TABLE>>> <<<DESCRIPTION>>> <<<TMP_ATHENA_BUCKET>>>