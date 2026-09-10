.PHONY: test vet compose-up compose-down terraform-fmt

test:
	go test ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/...

vet:
	go vet ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/...

compose-up:
	docker compose -f ledgerservice/compose.yaml up --build

compose-down:
	docker compose -f ledgerservice/compose.yaml down

terraform-fmt:
	terraform -chdir=infra/aws fmt -recursive

