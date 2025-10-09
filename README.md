# GossipingSystemUtilization
Progetto SDCC - GossipingSystemUtilization

Il progetto tratta dello siluppo di un'architettura basata su protocollo Gossip di una rete P2P tramite connessioni gRPC per System Utilization e Job Sharing tra i nodi.
L'architettura può sostenere fino a 20 nodi di cui 3 Seed e simula l'esecuzione di un sistema distribuito senza necessità di comandi manuali.

Meccanismi implementati: Piggyback - Friends&Affinity - Fault dei nodi - Leave volontaria dei nodi - SWIM per PING e PINGREQ - Messaggi diretti per PROBE, CANCEL, COMMIT dei job.

Ogni nodo viene generato da numerosi bucket di probabilità che ne descrivono la sua natura in funzione di:
  - Frequenza e durata delle Fault
  - Frequenza e durata delle Leave volontarie
  - Frequenza e preferenza di generazione dei job
  - Potenza e disponibilità delle risorse hardware

All'interno dell'architettura i nodi scambiano le proprie conoscenze della rete, direttamente e indirettamente, attraverso Gossiping. Venendo a conoscenza delle disponibilità hardware dei propri simili decidono in funzione dello score assegnato e della disponibilità a quale nodo inviare la richiesta di accettazione del job.
Tramite PING e PINGREQ i nodi possono monitorare lo stato di vita del sistema passando lo stato di un nodo che non risponde da ALIVE a SUSPECT e successivamente da SUSPECT a DEAD.
Quando un nodo effettua una Leave volontaria lo comunica ad al più 3 nodi appartenenti alla sua Membership List prima di disconnettersi, i nodi che ricevono la comunicazione di Leave la diffonderanno poi agli altri nodi della rete.

Ogni nodo è rappresentato da un container Docker per garantire isolamento e la corrette emulazione come fosse un sistema distribuito reale.

Comandi per l'esecuzione Docker:

Automatica:
1) make build
2) make up ---> avvia in automatico i container e l'escuzione del nodo
3) make logs-peerNUM o make logs-seedNUM --> connette al container per vedere logs di esecuzione già iniziata del nodo

Manuale:
1) make build
2) make up-manual ----> avvia in automatico i container ma non l'esecuzione del nodo
3) make sh-peerNUM o make sh-seedNUM ----> connessione al container
4) /app/entrypoint.sh ---> avvia l'esecuzione del nodo con logs

Rimozione:
 - docker compose down ---> ferma esecuzione e rimuove container
 - docker compose stop ---> ferma esecuzione
