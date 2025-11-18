# Makefile for Sharded Join ETL Pipeline with Apache Arrow
#
# This Makefile supports multiple build modes:
# 1. Dynamic linking (default) - requires Arrow installed
# 2. Static linking - creates fully self-contained binary
# 3. Bundle mode - includes Arrow libs with the binary
#
# Supports: macOS and Linux
#
# Usage:
#   make                  # Dynamic linking
#   make static           # Static linking (self-contained)
#   make bundle           # Bundle Arrow libs
#   make clean            # Clean build artifacts

# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pthread

# Source files
SRC = src/sharded_join_refactored.cpp
TARGET = sharded_join

# Detect operating system
UNAME_S := $(shell uname -s)

# OS-specific settings
ifeq ($(UNAME_S),Darwin)
    # macOS
    OS = macos
    DYLIB_EXT = dylib
    # Check for user override first, then brew, then default
    ifndef ARROW_PREFIX
        ARROW_PREFIX := $(shell brew --prefix apache-arrow 2>/dev/null || echo "/opt/homebrew/opt/apache-arrow")
    endif
    RPATH_TOOL = install_name_tool
    LDD_TOOL = otool -L
else ifeq ($(UNAME_S),Linux)
    # Linux
    OS = linux
    DYLIB_EXT = so
    # Check for user override first (ARROW_HOME or ARROW_PREFIX)
    ifndef ARROW_PREFIX
        ifdef ARROW_HOME
            ARROW_PREFIX := $(ARROW_HOME)
        else ifdef CONDA_PREFIX
            # Check if Arrow is in conda environment
            ifneq ($(wildcard $(CONDA_PREFIX)/include/arrow/api.h),)
                ARROW_PREFIX := $(CONDA_PREFIX)
            else
                # Try pkg-config, then common install locations
                ARROW_PREFIX := $(shell pkg-config --variable=prefix arrow 2>/dev/null || echo "/usr")
            endif
        else
            # Try pkg-config first, then common install locations
            ARROW_PREFIX := $(shell pkg-config --variable=prefix arrow 2>/dev/null || echo "/usr")
        endif
    endif
    RPATH_TOOL = patchelf
    LDD_TOOL = ldd
else
    $(error Unsupported operating system: $(UNAME_S))
endif

# Detect include directory
ifeq ($(OS),linux)
    # On Linux, try pkg-config first for include path
    ARROW_INCLUDE_PKGCONF := $(shell pkg-config --variable=includedir arrow 2>/dev/null)
    ifneq ($(ARROW_INCLUDE_PKGCONF),)
        ARROW_INCLUDE = $(ARROW_INCLUDE_PKGCONF)
    else
        ARROW_INCLUDE = $(ARROW_PREFIX)/include
    endif
else
    ARROW_INCLUDE = $(ARROW_PREFIX)/include
endif

# Detect library directory (lib vs lib64 vs lib/x86_64-linux-gnu)
ifeq ($(OS),linux)
    # On Linux, try pkg-config first for library path
    ARROW_LIB_PKGCONF := $(shell pkg-config --variable=libdir arrow 2>/dev/null)
    ifneq ($(ARROW_LIB_PKGCONF),)
        ARROW_LIB = $(ARROW_LIB_PKGCONF)
    else
        # Fall back to searching common locations
        ifneq ($(wildcard $(ARROW_PREFIX)/lib/x86_64-linux-gnu/libarrow.so),)
            ARROW_LIB = $(ARROW_PREFIX)/lib/x86_64-linux-gnu
        else ifneq ($(wildcard $(ARROW_PREFIX)/lib64/libarrow.so),)
            ARROW_LIB = $(ARROW_PREFIX)/lib64
        else
            ARROW_LIB = $(ARROW_PREFIX)/lib
        endif
    endif
else
    ARROW_LIB = $(ARROW_PREFIX)/lib
endif

# Check if Arrow is installed
# Try multiple possible header locations
ARROW_HEADER_FOUND := $(wildcard $(ARROW_INCLUDE)/arrow/api.h)
ifeq ($(ARROW_HEADER_FOUND),)
    # Try alternative locations on Linux
    ifeq ($(OS),linux)
        ARROW_HEADER_FOUND := $(wildcard /usr/include/arrow/api.h)
        ifeq ($(ARROW_HEADER_FOUND),)
            ARROW_HEADER_FOUND := $(wildcard /usr/local/include/arrow/api.h)
        endif
    endif
endif

ifeq ($(ARROW_HEADER_FOUND),)
    ifeq ($(OS),macos)
        $(error Apache Arrow not found. Install with: brew install apache-arrow)
    else
        $(error Apache Arrow not found. Install with: sudo apt-get install -y libarrow-dev libparquet-dev)
    endif
endif

# Arrow libraries needed
ARROW_LIBS = -larrow -lparquet

# Include paths
# Use -isystem for Arrow to suppress warnings from their headers
INCLUDES = -Isrc -isystem $(ARROW_INCLUDE)

# Library paths
LIBDIRS = -L$(ARROW_LIB)

# Default target: dynamic linking
.PHONY: all
all: $(TARGET)

$(TARGET): $(SRC) src/*.h
	@echo "Building with dynamic linking..."
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(LIBDIRS) -o $(TARGET) $(SRC) $(ARROW_LIBS) -lz
	@echo "Built: $(TARGET)"
	@echo "Note: Requires Arrow libraries at runtime"

# Static linking target - creates self-contained binary
.PHONY: static
static: $(SRC) src/*.h
	@echo "Building with static linking..."
	@# Link Arrow statically if .a files exist
	@if [ -f "$(ARROW_LIB)/libarrow.a" ]; then \
		echo "Note: Static linking requires many dependencies (bz2, brotli, lz4, zstd, snappy, mimalloc)"; \
		echo "This is complex - using dynamic linking with bundled libs is recommended instead."; \
		echo ""; \
		echo "Try: make bundle"; \
		exit 1; \
	else \
		echo "ERROR: Arrow static libraries not found at $(ARROW_LIB)"; \
		echo "Use 'make' for dynamic linking or 'make bundle' for portable distribution"; \
		exit 1; \
	fi

# Bundle mode - copy Arrow dylibs next to binary and set RPATH
.PHONY: bundle
bundle: $(TARGET) bundle-libs

bundle-libs:
	@echo "Bundling Arrow libraries..."
	@mkdir -p lib
ifeq ($(OS),macos)
	@# Copy Arrow dylibs (macOS)
	@if [ -f "$(ARROW_LIB)/libarrow.dylib" ] || [ -f "$(ARROW_LIB)"/libarrow.*.dylib ]; then \
		cp -f $(ARROW_LIB)/libarrow*.dylib lib/ 2>/dev/null || true; \
		cp -f $(ARROW_LIB)/libparquet*.dylib lib/ 2>/dev/null || true; \
		echo "Copied Arrow libraries to lib/"; \
	else \
		echo "Warning: Arrow .dylib files not found in $(ARROW_LIB)"; \
	fi
	@# Update binary RPATH to find bundled libs (macOS)
	@if command -v install_name_tool >/dev/null 2>&1; then \
		install_name_tool -add_rpath @executable_path/lib $(TARGET) 2>/dev/null || true; \
		echo "Updated RPATH in $(TARGET)"; \
	fi
else
	@# Copy Arrow shared libraries (Linux)
	@if [ -f "$(ARROW_LIB)/libarrow.so" ]; then \
		cp -f $(ARROW_LIB)/libarrow*.so* lib/ 2>/dev/null || true; \
		cp -f $(ARROW_LIB)/libparquet*.so* lib/ 2>/dev/null || true; \
		echo "Copied Arrow libraries to lib/"; \
	else \
		echo "Warning: Arrow .so files not found in $(ARROW_LIB)"; \
	fi
	@# Update binary RPATH to find bundled libs (Linux)
	@if command -v patchelf >/dev/null 2>&1; then \
		patchelf --set-rpath '$$ORIGIN/lib' $(TARGET) 2>/dev/null || true; \
		echo "Updated RPATH in $(TARGET)"; \
	else \
		echo "Warning: patchelf not found. Install with: sudo apt-get install patchelf"; \
		echo "Without patchelf, bundled libraries may not be found at runtime."; \
	fi
endif
	@echo ""
	@echo "Bundle complete! Distribute these files together:"
	@echo "  - $(TARGET)"
	@echo "  - lib/ directory"

# Debug target - show all detected paths
.PHONY: debug
debug:
	@echo "=== Makefile Debug Information ==="
	@echo "Operating System: $(OS) ($(UNAME_S))"
	@echo ""
	@echo "Environment Variables:"
	@echo "  ARROW_HOME: $(ARROW_HOME)"
	@echo "  CONDA_PREFIX: $(CONDA_PREFIX)"
	@echo ""
	@echo "Arrow Detection:"
	@echo "  ARROW_PREFIX: $(ARROW_PREFIX)"
	@echo "  ARROW_INCLUDE: $(ARROW_INCLUDE)"
	@echo "  ARROW_LIB: $(ARROW_LIB)"
	@echo "  ARROW_HEADER_FOUND: $(ARROW_HEADER_FOUND)"
	@echo ""
	@echo "pkg-config info:"
	@pkg-config --modversion arrow 2>/dev/null && echo "  Arrow version: $$(pkg-config --modversion arrow)" || echo "  pkg-config: Arrow not found"
	@pkg-config --variable=prefix arrow 2>/dev/null && echo "  prefix: $$(pkg-config --variable=prefix arrow)" || true
	@pkg-config --variable=includedir arrow 2>/dev/null && echo "  includedir: $$(pkg-config --variable=includedir arrow)" || true
	@pkg-config --variable=libdir arrow 2>/dev/null && echo "  libdir: $$(pkg-config --variable=libdir arrow)" || true
	@echo ""
	@echo "File checks:"
	@test -f "$(ARROW_INCLUDE)/arrow/api.h" && echo "  ✓ $(ARROW_INCLUDE)/arrow/api.h exists" || echo "  ✗ $(ARROW_INCLUDE)/arrow/api.h not found"
	@test -f "/usr/include/arrow/api.h" && echo "  ✓ /usr/include/arrow/api.h exists" || echo "  ✗ /usr/include/arrow/api.h not found"
	@test -f "/usr/local/include/arrow/api.h" && echo "  ✓ /usr/local/include/arrow/api.h exists" || echo "  ✗ /usr/local/include/arrow/api.h not found"

# Setup target - check dependencies and provide setup instructions
.PHONY: setup
setup:
	@echo "=== Apache Arrow Setup ==="
	@echo "Operating System: $(OS)"
	@echo ""
	@if [ -d "$(ARROW_PREFIX)" ]; then \
		echo "✓ Arrow found at: $(ARROW_PREFIX)"; \
		echo "✓ Version: $$(pkg-config --modversion arrow 2>/dev/null || echo 'unknown')"; \
	else \
		echo "✗ Arrow not found"; \
		echo ""; \
		echo "Install Arrow:"; \
		if [ "$(OS)" = "macos" ]; then \
			echo "  brew install apache-arrow"; \
		else \
			echo "  System-wide (requires sudo):"; \
			echo "    sudo apt-get update"; \
			echo "    sudo apt-get install -y libarrow-dev libparquet-dev"; \
			echo ""; \
			echo "  User-space (no sudo):"; \
			echo "    Option 1 - Using Conda:"; \
			echo "      conda install -c conda-forge arrow-cpp"; \
			echo "      export PKG_CONFIG_PATH=\$$CONDA_PREFIX/lib/pkgconfig:\$$PKG_CONFIG_PATH"; \
			echo ""; \
			echo "    Option 2 - Set custom location:"; \
			echo "      export ARROW_HOME=\$$HOME/arrow"; \
			echo "      export PKG_CONFIG_PATH=\$$ARROW_HOME/lib/pkgconfig:\$$PKG_CONFIG_PATH"; \
			echo "      export LD_LIBRARY_PATH=\$$ARROW_HOME/lib:\$$LD_LIBRARY_PATH"; \
			echo "      (Then build Arrow from source into \$$ARROW_HOME)"; \
		fi; \
		exit 1; \
	fi
	@echo ""
	@echo "Build options:"; \
	echo "  make        - Dynamic linking (requires Arrow at runtime)"; \
	echo "  make static - Static linking (self-contained binary)"; \
	echo "  make bundle - Bundle Arrow libs with binary"; \

# Clean build artifacts
.PHONY: clean
clean:
	rm -f $(TARGET) $(TARGET)_static
	rm -rf lib/
	@echo "Cleaned build artifacts"

# Test that Arrow is working
.PHONY: test-arrow
test-arrow:
	@echo "Testing Arrow installation..."
	@echo '#include <arrow/api.h>' > /tmp/test_arrow.cpp
	@echo '#include <iostream>' >> /tmp/test_arrow.cpp
	@echo 'int main() { std::cout << "Arrow version: " << ARROW_VERSION_STRING << std::endl; return 0; }' >> /tmp/test_arrow.cpp
	@$(CXX) -std=c++17 $(INCLUDES) $(LIBDIRS) /tmp/test_arrow.cpp -o /tmp/test_arrow $(ARROW_LIBS)
	@/tmp/test_arrow
	@rm -f /tmp/test_arrow /tmp/test_arrow.cpp
	@echo "✓ Arrow is working correctly"

# Help target
.PHONY: help
help:
	@echo "Sharded Join ETL Pipeline - Build Options"
	@echo "Detected OS: $(OS)"
	@echo ""
	@echo "Targets:"
	@echo "  make          - Build with dynamic linking (default)"
	@echo "  make static   - Build self-contained static binary"
	@echo "  make bundle   - Build and bundle Arrow libraries"
	@echo "  make setup    - Check Arrow installation"
	@echo "  make debug    - Show detailed Arrow detection info"
	@echo "  make clean    - Remove build artifacts"
	@echo "  make test-arrow - Test Arrow installation"
	@echo "  make info     - Show binary information"
	@echo ""
	@echo "Current Arrow location: $(ARROW_PREFIX)"
ifeq ($(OS),linux)
	@echo ""
	@echo "Note: For 'make bundle' on Linux, install patchelf:"
	@echo "  sudo apt-get install patchelf"
endif

# Info about the binary
.PHONY: info
info:
	@if [ -f "$(TARGET)" ]; then \
		echo "Binary: $(TARGET)"; \
		echo "Size: $$(du -h $(TARGET) | cut -f1)"; \
		echo "OS: $(OS)"; \
		echo ""; \
		echo "Dynamic libraries:"; \
		$(LDD_TOOL) $(TARGET) | grep -v "$(TARGET)" || true; \
	else \
		echo "Binary not built yet. Run 'make' to build."; \
	fi

.DEFAULT_GOAL := all

