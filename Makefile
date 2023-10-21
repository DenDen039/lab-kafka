.PHONY: run
run:
	docker-compose up -d
	docker attach consumer

.PHONY: stop
stop:
	docker-compose down -v

.PHONY: build
build:
	cd ./consumer && docker image build . --tag lab/consumer:latest;
	cd ./producer && docker image build . --tag lab/producer:latest;
