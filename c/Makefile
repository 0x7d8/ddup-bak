# Makefile for building and installing libddupbak

# Variables
TARGET_DIR = target/release
LIB_NAME = libddupbak.so
GLOBAL_DIR = /usr/local/lib
INCLUDE_DIR = /usr/local/include
HEADER_FILE = libddupbak.h

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

$(HEADER_FILE):
	@echo "Copying header file..."

install: $(TARGET_DIR)/$(LIB_NAME) $(HEADER_FILE)
	@echo "Installing $(LIB_NAME) to $(GLOBAL_DIR)..."
	install -D $(TARGET_DIR)/$(LIB_NAME) $(GLOBAL_DIR)/$(LIB_NAME)
	@echo "Installing $(HEADER_FILE) to $(INCLUDE_DIR)..."
	install -D include/$(HEADER_FILE) $(INCLUDE_DIR)/$(HEADER_FILE)

clean:
	@echo "Cleaning build artifacts..."
	cargo clean

.PHONY: all install clean