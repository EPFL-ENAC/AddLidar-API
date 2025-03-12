install:
	$(MAKE) -C lidar-api install
run:
	$(MAKE) -C lidar-api run
test:
	$(MAKE) -C lidar-api test
format:
	$(MAKE) -C lidar-api format
lint:
	$(MAKE) -C lidar-api lint
	
# Lefthook commands
install-lefthook:
	@echo "Installing lefthook..."
	@if command -v npm >/dev/null 2>&1; then \
		npm install -g lefthook; \
	else \
		echo "Error: npm not found. Please install Node.js and npm."; \
		exit 1; \
	fi
	@echo "Lefthook installed successfully!"

init-lefthook:
	@echo "Initializing lefthook..."
	@if command -v lefthook >/dev/null 2>&1; then \
		lefthook install; \
	else \
		echo "Error: lefthook not found. Please run 'make install-lefthook' first."; \
		exit 1; \
	fi
	@echo "Git hooks installed successfully!"

setup-hooks: install-lefthook init-lefthook