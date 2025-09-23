BIN ?= dkit
PKG := ./cmd/dkit
OUT := bin/$(BIN)

.PHONY: build run clean
build:
	go build -o $(OUT) $(PKG)

run:
	go run $(PKG)

clean:
	rm -f $(OUT)
