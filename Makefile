NETWORK_NAME=harvey-cloud

.PHONY: create-network

create-network:
	@if [ -z "$$(docker network ls --filter name=^$(NETWORK_NAME)$$ --format '{{ .Name }}')" ]; then \
		echo "Creating Docker network: $(NETWORK_NAME)"; \
		docker network create $(NETWORK_NAME); \
	else \
		echo "Docker network '$(NETWORK_NAME)' already exists."; \
	fi

MODULES = module_postgresql_db module_metabase module_local_s3 module_pyspark_cluster module_dbt
build-all-modules:
	@for module in $(MODULES); do \
		echo "Starting $$module..."; \
		cd $$module && docker compose up -d && cd ..; \
	done

# Special handling for Airflow (2x up due to initialization)
build-airflow:
	@echo "Starting module_airflow (init)..."
	cd module_airflow && docker compose up -d
	sleep 10
	@echo "Starting airflow_webserver..."
	cd module_airflow && docker compose up -d && cd ..
	sleep 10

migrate-dm:
	@echo "Starting module_migration_dm..."
	cd module_migration_dm && docker compose up -d

start: create-network build-all-modules build-airflow migrate-dm
	@echo "All modules started."

# Optional cleanup
clean:
	@for module in $(MODULES) module_airflow module_migration_dm; do \
		echo "Stopping $$module..."; \
		cd $$module && docker compose down && cd ..; \
	done
	docker network rm $(NETWORK_NAME) || true
