NETWORK_NAME=harvey-cloud

.PHONY: create-network

create-network:
	@if [ -z "$$(docker network ls --filter name=^$(NETWORK_NAME)$$ --format '{{ .Name }}')" ]; then \
		echo "Creating Docker network: $(NETWORK_NAME)"; \
		docker network create $(NETWORK_NAME); \
	else \
		echo "Docker network '$(NETWORK_NAME)' already exists."; \
	fi

