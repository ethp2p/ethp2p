# Default recipe
default:
    @just --list

# Regenerate protobufs and build all Go packages
build:
    buf generate
    go build ./...

# Regenerate protobufs only
proto:
    buf generate

# Lint proto files
proto-lint:
    buf lint

# Install Python CLI
install-cli:
    cd sim/cli && uv sync

# Install spec tooling and format specs
specs:
    if ! command -v rumdl >/dev/null 2>&1; then uv tool install rumdl; fi
    rumdl fmt specs/

# Generate a topology
topology n="10" d="4" seed="42" out="topology.json":
    simctl topology -n {{n}} -d {{d}} -s {{seed}} -o {{out}}

# Run simnet tests
test-simnet:
    go test ./sim/... -v

# Run simnet tests (short mode)
test-simnet-short:
    go test ./sim/... -v -short

# Run a Shadow simulation
shadow config="config.yaml":
    uv run --project sim/cli simctl run {{config}} --mode=shadow

# Build the simnode binary
build-simnode:
    go build -o sim/simcfg/simnode/simnode ./sim/simcfg/simnode

# Generate default config
init-config out="config.yaml":
    simctl init -o {{out}}

# Open analysis notebook
analyze:
    cd sim/analysis && jupyter notebook broadcast-analysis.ipynb

# Clean generated files
clean:
    rm -rf runs/
    rm -f topology.json config.yaml
    rm -f sim/simcfg/simnode/simnode
