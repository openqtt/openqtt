# OpenQTT 2.0. `make check` is the gate: CI runs exactly it, and a pull request is ready when it
# passes locally. Every step of it also runs alone.

CARGO ?= cargo

.PHONY: check fmt fmt-check clippy test test-crate layers deny mdlint conformance tools need-nextest \
	need-deny

check: fmt-check clippy test layers deny mdlint
	@echo "check: ok"

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all --check

clippy:
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings

# nextest runs the unit and integration tests but not doctests, so cargo test runs those.
test: need-nextest
	$(CARGO) nextest run --workspace --locked --no-fail-fast
	$(CARGO) test --doc --workspace --locked

# One crate's tests, or the tests in it whose name contains T:
#   make test-crate P=config T=unicode
test-crate: need-nextest
	$(CARGO) nextest run --locked -p openqtt-$(P) $(T)

# Licences, advisories, banned crates and sources, as deny.toml sets them.
deny: need-deny
	$(CARGO) deny --locked check

# --- Layers -------------------------------------------------------------------------------
#
# Dependencies point down the crate table (docs/README.md; report R3 is normative). Each rule
# names what a crate must never reach through its normal dependencies, on any target, so a
# platform-specific dependency cannot slip past a check run on another platform. Dev and build
# dependencies are not counted: a test may use what the crate may not.

IO := tokio|quinn|rustls|mio|socket2|hyper|reqwest|axum
STORAGE := fjall|redb|rocksdb|librocksdb-sys|sled|heed|lmdb-rkv
IMPLEMENTATIONS := openqtt-(auth|session|config|observe|wire|transport|cluster|log|router|edge|admin|node|client|testkit)

# One line per package, `name vX.Y.Z`, without the tree drawing.
deps = $(CARGO) tree --locked --edges normal --target all --prefix none --format '{p}'

# $(call forbid,<crate>,<names>), where <names> is an extended regex of crate names.
forbid = @out=$$($(deps) -p $(1)) || exit 1; \
	bad=$$(echo "$$out" | sed 1d | grep -E '^($(2)) ' | sort -u); \
	if [ -n "$$bad" ]; then echo "$$bad"; echo "error: $(1) must not depend on $(2)"; exit 1; fi

layers:
	$(call forbid,openqtt-codec,tokio|quinn|rustls|serde)
	$(call forbid,openqtt-topic,openqtt-codec)
	$(call forbid,openqtt-core,$(IO))
	$(call forbid,openqtt-ext,$(IMPLEMENTATIONS))
	$(call forbid,openqtt-auth,hyper|reqwest)
	$(call forbid,openqtt-session,$(IO))
	$(call forbid,openqtt-config,tokio)
	$(call forbid,openqtt-observe,opentelemetry_sdk|tracing-subscriber)
	$(call forbid,openqtt-wire,openqtt-codec|openqtt-session|openqtt-auth)
	$(call forbid,openqtt-transport,openqtt-session|openqtt-wire|openqtt-auth)
	$(call forbid,openqtt-cluster,openqtt-codec|openqtt-session|openqtt-transport)
	$(call forbid,openqtt-log,openqtt-codec|openqtt-session|openqtt-transport|axum)
	$(call forbid,openqtt-router,openqtt-codec|openqtt-session|$(STORAGE))
	$(call forbid,openqtt-edge,openraft|axum|$(STORAGE))
	$(call forbid,openqtt-admin,openqtt-codec|openqtt-session|openqtt-transport)
	$(call forbid,openqtt-client,openqtt-session)
	@# The test kit is a dev-dependency only: nothing reaches it through a normal dependency.
	@out=$$($(deps) --invert openqtt-testkit --workspace) || exit 1; \
	users=$$(echo "$$out" | sed 1d | sort -u); \
	if [ -n "$$users" ]; then echo "$$users"; \
		echo "error: openqtt-testkit must only ever be a dev-dependency"; exit 1; fi
	@# One TLS stack and one crypto provider in the binary: at most one rustls (exactly one once
	@# QUIC lands), never ring, and aws-lc-rs whenever rustls is there.
	@out=$$($(deps) -p openqtt) || exit 1; \
	rustls=$$(echo "$$out" | grep -E '^rustls ' | cut -d' ' -f2 | sort -u); \
	n=$$(echo "$$rustls" | grep -c .); \
	if [ "$$n" -gt 1 ]; then echo "$$rustls"; \
		echo "error: openqtt links $$n versions of rustls; there must be one"; exit 1; fi; \
	if echo "$$out" | grep -E '^ring '; then \
		echo "error: openqtt links ring; aws-lc-rs is the only crypto provider"; exit 1; fi; \
	if [ "$$n" -eq 1 ] && ! echo "$$out" | grep -qE '^aws-lc-rs '; then \
		echo "error: openqtt links rustls without aws-lc-rs"; exit 1; fi; \
	echo "layers: ok (rustls versions in openqtt: $$n)"

# --- Markdown -----------------------------------------------------------------------------

# No em dash (U+2014) in any tracked Markdown file. Written as bytes so this file has none.
EM_DASH := $(shell printf '\342\200\224')

mdlint:
	@if git ls-files -z -- '*.md' | LC_ALL=C xargs -0 grep -n -- '$(EM_DASH)'; then \
		echo "error: em dash in Markdown; use a colon, a comma or a full stop"; exit 1; fi
	@echo "mdlint: ok"

# --- Conformance --------------------------------------------------------------------------

# The MQTT 5.0 statements in report R1 that no test names yet, one per line. Not part of
# check: today it lists every statement. R1 says how a test names the statements it proves.
conformance:
	@scripts/conformance-ids.sh

# --- Tools --------------------------------------------------------------------------------

# The two cargo tools the gate needs beyond the toolchain.
tools:
	$(CARGO) install --locked cargo-nextest cargo-deny

need-nextest:
	@command -v cargo-nextest >/dev/null 2>&1 || { \
		echo "error: cargo-nextest is not installed. Install it with"; \
		echo "    cargo install --locked cargo-nextest    (or: make tools)"; exit 1; }

need-deny:
	@command -v cargo-deny >/dev/null 2>&1 || { \
		echo "error: cargo-deny is not installed. Install it with"; \
		echo "    cargo install --locked cargo-deny    (or: make tools)"; exit 1; }
