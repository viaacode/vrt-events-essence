VERSION = ${shell (git describe --tags || echo latest)}

.ONESHELL:
SHELL = /bin/bash

.PHONY: all

NAMESPACE = vrt-intake
OC_PROJECT = ${NAMESPACE}

APP_NAME = vrt-events-essence
IMAGE_NAME = ${NAMESPACE}/${APP_NAME}
REGISTRY = docker-registry-default.apps.do-prd-okp-m0.do.viaa.be
login:
	login_oc.sh https://do-prd-okp-m0.do.viaa.be:8443
	oc project "${OC_PROJECT}"
build:
	cp ./config.yml.example ./config.yml
	docker build -t "${REGISTRY}/${IMAGE_NAME}:${VERSION}" .
test:
	docker container run --name "${APP_NAME}_test" \
					--entrypoint python "${REGISTRY}/${IMAGE_NAME}:${VERSION}" \
					"-m" "pytest" "--cov=./app" "--junit-xml=./tests/test_results.xml"
lint:
	docker container run --name "${APP_NAME}_lint" \
					--entrypoint flake8 "${REGISTRY}/${IMAGE_NAME}:${VERSION}" \
					--exit-zero \
					--max-line-length=88
push: 	login
	docker push ${REGISTRY}/${IMAGE_NAME}:${VERSION}

tag-int:
	oc tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:int
deploy-int:
test-int:

tag-qas:
	oc tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:qas
deploy-qas:
	bash ./openshift/wait4rollout.sh qas ${APP_NAME}
test-qas:

tag-prd:
	oc tag ${IMAGE_NAME}:${VERSION} ${IMAGE_NAME}:prd
deploy-prd:

approved:
	echo "Approved"

post-clean:
	docker container rm "${APP_NAME}_test"
	docker container rm "${APP_NAME}_lint"
