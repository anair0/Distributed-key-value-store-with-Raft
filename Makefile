.PHONY: build test race vet fmt ci run docker bench

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race -count=1 -timeout 20m ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

ci: vet race

# 3-node cluster + dashboard on localhost (http://127.0.0.1:8080)
run:
	scripts/run-local.sh

# 3-node cluster + dashboard + Prometheus + Grafana
docker:
	docker compose -f deploy/docker-compose.yml up --build

bench:
	go test -run '^$$' -bench . ./internal/lsm/
