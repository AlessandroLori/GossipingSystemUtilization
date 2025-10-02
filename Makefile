# --- Variabili ---
COMPOSE := docker compose
BASE := -f docker-compose.yml
MANUAL := -f docker-compose.yml -f docker-compose.manual.yml
PEER_SCALE ?= 1
SEEDS := seed1 seed2 seed3

# --- Phony ---
.PHONY: build build-nocache up up-auto up-manual ps logs logs-% stop down clean \
        sh-seed1 sh-seed2 sh-seed3 sh-peer% \
        start-seed1 start-seed2 start-seed3 start-peer% start-peers start-seeds \
        restart restart-%

# --- BUILD: solo immagini, non avvia nulla ---
build:
	$(COMPOSE) build

build-nocache:
	$(COMPOSE) build --no-cache

# --- UP AUTOMATICO: i container partono e l'app si avvia (ENTRYPOINT) ---
up:
	$(COMPOSE) $(BASE) up -d --scale peer=$(PEER_SCALE)

# alias
up-auto: up

# --- UP MANUALE: container accesi ma senza app (sleep infinity da override) ---
up-manual:
	$(COMPOSE) $(MANUAL) up -d --scale peer=$(PEER_SCALE)

# --- Stato & Log ---
ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

logs-%:
	$(COMPOSE) logs -f $*

# --- Stop / Down / Clean ---
stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down --remove-orphans

clean:
	$(COMPOSE) down -v --remove-orphans
	docker image prune -f

restart:
	$(COMPOSE) restart

restart-%:
	$(COMPOSE) restart $*

# --- Shell nei container ---
sh-seed1:
	$(COMPOSE) exec seed1 sh
sh-seed2:
	$(COMPOSE) exec seed2 sh
sh-seed3:
	$(COMPOSE) exec seed3 sh
# per i peer scalati: sh-peer1, sh-peer2, ...
sh-peer%:
	$(COMPOSE) exec --index $* peer sh

# --- AVVIO MANUALE DELL’APP DENTRO I CONTAINER ---
# evita doppio avvio (porta occupata) se già in esecuzione
start-seed1:
	$(COMPOSE) exec seed1 sh -lc 'pgrep -x node >/dev/null && echo "[seed1] già in esecuzione" || exec /app/entrypoint.sh'

start-seed2:
	$(COMPOSE) exec seed2 sh -lc 'pgrep -x node >/dev/null && echo "[seed2] già in esecuzione" || exec /app/entrypoint.sh'

start-seed3:
	$(COMPOSE) exec seed3 sh -lc 'pgrep -x node >/dev/null && echo "[seed3] già in esecuzione" || exec /app/entrypoint.sh'

# Avvia un peer specifico per indice: es. "make start-peer-2"
start-peer-%:
	$(COMPOSE) exec --index $* peer sh -lc 'pgrep -x node >/dev/null && echo "[peer-$*] già in esecuzione" || exec /app/entrypoint.sh'

# Avvia tutti i seed dichiarati sopra
start-seeds:
	@for s in $(SEEDS); do \
		echo ">> Avvio $$s"; \
		$(COMPOSE) exec $$s sh -lc 'pgrep -x node >/dev/null && echo "[$$s] già in esecuzione" || exec /app/entrypoint.sh'; \
	done

# Avvia tutti i peer presenti (1..PEER_SCALE)
start-peers:
	@i=1; while [ $$i -le $(PEER_SCALE) ]; do \
	  echo ">> Avvio peer $$i"; \
	  $(COMPOSE) exec --index $$i peer sh -lc 'pgrep -x node >/dev/null && echo "[peer-$$i] già in esecuzione" || exec /app/entrypoint.sh'; \
	  i=$$((i+1)); \
	done
