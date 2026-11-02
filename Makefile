# Docker Compose shortcuts for local development. Run them from the repository root.
#
# The compose file lives in infra/ and Compose reads a `.env` file from THE COMPOSE FILE'S folder
# (infra/), not from here, so every target passes `--env-file .env` to use the one in the repository
# root. Do not use `--project-directory .`: the compose file's relative paths (build context,
# volumes) assume infra/.
#
# Create .env first (see the README quickstart):
#   printf 'JWT_SECRET_KEY=%s\nENVIRONMENT=development\n' "$(openssl rand -hex 32)" > .env

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env

.PHONY: up down ps logs

# Build and start PostgreSQL and the API in the background.
up: .env
	$(COMPOSE) up -d --build

# Stop and remove the containers (the database files in infra/volumes are kept). This needs no real
# secret: the compose file insists on JWT_SECRET_KEY for every command, so a dummy fills in when
# none is set, and it is never used to start anything.
down: .env
	JWT_SECRET_KEY="$${JWT_SECRET_KEY:-unused-by-down}" $(COMPOSE) down

# Show the state of the containers.
ps: .env
	$(COMPOSE) ps

# Follow the logs of all services; add a service name with `make logs S=api`.
logs: .env
	$(COMPOSE) logs -f --tail=100 $(S)

.env:
	@echo "No .env in the repository root. Create it first; the README quickstart has the one-line command." >&2
	@exit 1
