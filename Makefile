# ===== Makefile per GossipSystemUtilization =====
SHELL := /bin/bash

GO      := go
PROTOC  := protoc
GOPATH  := $(shell $(GO) env GOPATH)

# Plugin protoc (installati con: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#                                     go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest)
PROTOC_GEN_GO       := $(GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC  := $(GOPATH)/bin/protoc-gen-go-grpc

# Proto
PROTO_DIR := proto
PROTO_SRC := $(PROTO_DIR)/gossip.proto
PROTO_GEN := $(PROTO_DIR)/gossip.pb.go $(PROTO_DIR)/gossip_grpc.pb.go

# Parametri di run (override da CLI: make run-peer BOOT=30 PEER_ADDR=127.0.0.1:9002 SEEDS=127.0.0.1:9001)
CONFIG    ?= ./config.json
ADDR      ?= 127.0.0.1:9001
PEER_ADDR ?= 127.0.0.1:9002
SEEDS     ?= 127.0.0.1:9001
BOOT      ?= 0

.PHONY: all proto build clean tidy fmt vet regen check-plugins check-proto run-seed run-peer doctor

## Default: pulisci mod, genera .pb.go e builda
all: tidy proto build

## Verifica che protoc e plugin siano presenti
check-plugins:
	@command -v $(PROTOC) >/dev/null || { echo "ERR: protoc non trovato nel PATH"; exit 1; }
	@test -x "$(PROTOC_GEN_GO)" || { echo "ERR: protoc-gen-go non trovato a $(PROTOC_GEN_GO)"; exit 1; }
	@test -x "$(PROTOC_GEN_GO_GRPC)" || { echo "ERR: protoc-gen-go-grpc non trovato a $(PROTOC_GEN_GO_GRPC)"; exit 1; }

## Rigenera i file gRPC dal .proto (output dentro ./proto)
proto: check-plugins
	@echo ">> Rigenero stubs gRPC"
	@rm -f $(PROTO_GEN)
	@$(PROTOC) -I $(PROTO_DIR) \
	  --plugin=protoc-gen-go="$(PROTOC_GEN_GO)" \
	  --plugin=protoc-gen-go-grpc="$(PROTOC_GEN_GO_GRPC)" \
	  --go_out=$(PROTO_DIR) --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_DIR) --go-grpc_opt=paths=source_relative \
	  $(PROTO_SRC)
	@$(MAKE) check-proto

## Controllo di qualità sui file generati
check-proto:
	@grep -q 'type AvailBatch' $(PROTO_DIR)/gossip.pb.go || { echo "ERR: AvailBatch mancante in gossip.pb.go"; exit 1; }
	@grep -q 'ExchangeAvail(context' $(PROTO_DIR)/gossip_grpc.pb.go || { echo "ERR: ExchangeAvail mancante in gossip_grpc.pb.go"; exit 1; }
	@echo ">> proto OK"

## Pulizia cache Go e file generati
clean:
	@echo ">> Pulizia cache e file generati"
	@rm -f $(PROTO_GEN)
	@$(GO) clean -cache -testcache

## Pulizia + rigenerazione .proto
regen: clean proto

## Mod tidy/fmt/vet
tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

## Build progetto
build:
	$(GO) build ./...

## Avvio seed
run-seed:
	CONFIG_PATH=$(CONFIG) GRPC_ADDR=$(ADDR) IS_SEED=true SEEDS= BOOT_DELAY_SIM_S=$(BOOT) $(GO) run ./cmd/node

## Avvio peer
run-peer:
	CONFIG_PATH=$(CONFIG) GRPC_ADDR=$(PEER_ADDR) IS_SEED=false SEEDS=$(SEEDS) BOOT_DELAY_SIM_S=$(BOOT) $(GO) run ./cmd/node

## Controlli rapidi: import sbagliati che spesso rompono i tipi generati
doctor:
	@echo ">> Doctor: controllo import 'google.golang.org/protobuf/proto' in file che devono usare il tuo 'proto'"
	@! grep -R --line-number 'google.golang.org/protobuf/proto' internal/antientropy internal/seed internal/swim 2>/dev/null || { echo "ATTENZIONE: rimuovi quell'import o rinominalo (es. goproto)"; exit 1; }
	@echo ">> Doctor: controllo duplicati di gossip*_pb.go fuori da ./proto"
	@! find . -type f -name 'gossip*_pb.go' -not -path './proto/*' -print | grep . && { echo "ATTENZIONE: rimuovi i duplicati indicati sopra"; exit 1; } || true
	@echo ">> Doctor OK"
