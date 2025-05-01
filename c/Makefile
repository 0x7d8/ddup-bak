# Makefile for building and installing libddupbak

# Variables
TARGET_DIR = target/release
LIB_NAME = libddupbak.so
GLOBAL_DIR = /usr/local/lib

# Detect platform-specific shared library extension
ifeq ($(OS),Windows_NT)
	LIB_NAME = ddupbak.dll
else
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Darwin)
		LIB_NAME = libddupbak.dylib
	endif
endif

# Targets
all: $(TARGET_DIR)/$(LIB_NAME)

$(TARGET_DIR)/$(LIB_NAME):
	@echo "Building $(LIB_NAME)..."
	cargo build --release

install: $(TARGET_DIR)/$(LIB_NAME)
	@echo "Installing $(LIB_NAME) to $(GLOBAL_DIR)..."
	install -D $(TARGET_DIR)/$(LIB_NAME) $(GLOBAL_DIR)/$(LIB_NAME)

clean:
	@echo "Cleaning build artifacts..."
	cargo clean

.PHONY: all install clean