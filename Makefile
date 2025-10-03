# --- Variabili ---
COMPOSE := docker compose
BASE := -f docker-compose.yml
MANUAL := -f docker-compose.yml -f docker-compose.manual.yml

# Elenca qui i servizi peer che hai nel compose:
PEERS := peer1 peer2 peer3 peer4 peer5 peer6 peer7 peer8 peer9 peer10 peer11 peer12 peer13 peer14 peer15 peer16
SEEDS := seed1 seed2 seed3

# (opzionale) se nel compose hai un "name:" diverso, aggiornalo qui
PROJECT := gossipsystutil

# --- Phony ---
.PHONY: build build-nocache up up-auto up-manual up-seeds up-peers \
        ps logs logs-% logs-peer-% \
        stop stop-seeds stop-peers down clean restart restart-% \
        sh-% sh-peer-% \
        start-% start-seeds start-peers start-all

# --- BUILD: solo immagini, non avvia nulla ---
build:
	$(COMPOSE) build

build-nocache:
	$(COMPOSE) build --no-cache

# --- UP AUTOMATICO (ENTRYPOINT attivo) ---
# Avvia TUTTI i servizi elencati (seeds + peers)
up:
	$(COMPOSE) $(BASE) up -d $(SEEDS) $(PEERS)

# alias
up-auto: up

# --- UP MANUALE (override con sleep infinity) ---
up-manual:
	$(COMPOSE) $(MANUAL) up -d $(SEEDS) $(PEERS)

# Solo seeds / solo peers (comodo se vuoi avviarli in fasi)
up-seeds:
	$(COMPOSE) $(BASE) up -d $(SEEDS)

up-peers:
	$(COMPOSE) $(BASE) up -d $(PEERS)

# --- Stato & Log ---
ps:
	$(COMPOSE) ps

# log di un servizio qualsiasi (es. make logs-seed1, logs-peer7)
logs-%:
	$(COMPOSE) logs -f $*

# log di un peer per indice numerico: make logs-peer-7 -> servizio "peer7"
logs-peer-%:
	$(COMPOSE) logs -f peer$*

# tutti i log
logs:
	$(COMPOSE) logs -f

# --- Stop / Down / Clean ---
stop:
	$(COMPOSE) stop

stop-seeds:
	$(COMPOSE) stop $(SEEDS)

stop-peers:
	$(COMPOSE) stop $(PEERS)

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
# shell su un servizio qualsiasi (seed1, peer7, ecc.)
sh-%:
	$(COMPOSE) exec $* sh

# shell su peer per indice numerico: make sh-peer-7 -> "peer7"
sh-peer-%:
	$(COMPOSE) exec peer$* sh

# --- AVVIO MANUALE DELL’APP DENTRO I CONTAINER ---
# usa pgrep/pidof per evitare doppi avvii; se non gira, esegue l'entrypoint

start-%:
	$(COMPOSE) exec $* sh -lc '(pgrep -x node >/dev/null 2>&1 || pidof node >/dev/null 2>&1) && echo "[$*] già in esecuzione" || exec /app/entrypoint.sh'

start-seeds:
	@for s in $(SEEDS); do \
	  echo ">> Avvio $$s"; \
	  $(COMPOSE) exec $$s sh -lc '(pgrep -x node >/dev/null 2>&1 || pidof node >/dev/null 2>&1) && echo "[$$s] già in esecuzione" || exec /app/entrypoint.sh'; \
	done

start-peers:
	@for p in $(PEERS); do \
	  echo ">> Avvio $$p"; \
	  $(COMPOSE) exec $$p sh -lc '(pgrep -x node >/dev/null 2>&1 || pidof node >/dev/null 2>&1) && echo "[$$p] già in esecuzione" || exec /app/entrypoint.sh'; \
	done

start-all: start-seeds start-peers
