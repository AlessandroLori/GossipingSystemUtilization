#!/usr/bin/env python3

#CONFIG_PATH=./config.json IS_SEED=false GRPC_ADDR=127.0.0.1:9020 SEEDS=127.0.0.1:9004 go run ./cmd/node ---> configurazioni del nodo, peggior caso unico seed

import sys, csv, os, math
import matplotlib.pyplot as plt

def read_csv(path):
    t_s, disc_total = [], []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ms = int(row["ms"])
            t_s.append(ms / 1000.0)
            if "discovered_others" in row and row["discovered_others"] != "":
                disc_total.append(int(row["discovered_others"]) + 1)
            else:
                disc_total.append(int(row["discovered"]))
    return t_s, disc_total

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 plot_ttfd.py <path_csv> [EXPECTED_NODES] [output_png]")
        sys.exit(1)

    csv_path = sys.argv[1]
    expected = int(sys.argv[2]) if len(sys.argv) >= 3 else 20
    out_png = sys.argv[3] if len(sys.argv) >= 4 else os.path.splitext(csv_path)[0] + ".png"

    t_s, disc_total = read_csv(csv_path)
    y_other = [max(0, v - 1) for v in disc_total]   # esclude self
    target = max(0, expected - 1)

    # TTFD = primo t in cui raggiunge target
    t_tffd = None
    for t, y in zip(t_s, y_other):
        if y >= target:
            t_tffd = t
            break

    # ---- Plot “clean” ----
    plt.figure()
    plt.step(t_s, y_other, where="post", linewidth=2)   # niente marker/pallini
    plt.xlabel("Tempo (s)")
    plt.ylabel("Nodi scoperti")
    plt.title("Tempo di Full Discovery (ultimo nodo)")
    plt.grid(True)

    # Tick della Y solo su interi
    ymax = max([target] + y_other) if y_other else target
    plt.yticks(list(range(0, int(math.ceil(ymax)) + 1)))

    # Linee guida al target e al TTFD (senza testo verticale)
    if target > 0:
        plt.axhline(target, linestyle="--", linewidth=1)
    if t_tffd is not None:
        plt.axvline(t_tffd, linestyle="--", linewidth=1)

    plt.tight_layout()
    plt.savefig(out_png, dpi=160)

    print(f"✅ Plot salvato: {out_png}")
    last = y_other[-1] if y_other else 0
    if last >= target and t_tffd is not None:
        print(f"TTFD (altri {target}) ≈ {t_tffd:.3f} s")
    else:
        print(f"⚠️ Non ha raggiunto {target} nodi entro l'ultima misura. Ultimo valore: {last}/{target}")

if __name__ == "__main__":
    main()
