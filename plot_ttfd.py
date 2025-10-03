#!/usr/bin/env python3
import sys, csv, os, math
import matplotlib.pyplot as plt

def read_csv(path):
    """Ritorna (t_s, y_other) dal CSV con colonne ms,[discovered],[discovered_others]."""
    t_s, y_other = [], []
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ms = int(row["ms"]); t_s.append(ms / 1000.0)
            if "discovered_others" in row and row["discovered_others"] != "":
                y_other.append(int(row["discovered_others"]))
            else:
                total = int(row["discovered"])
                y_other.append(max(0, total - 1))
    return t_s, y_other

def _setup_axes(ax, expected, title=None):
    if title:
        ax.set_title(title)
    ax.set_xlabel("Tempo (s)")
    ax.set_ylabel("Nodi scoperti")
    ax.grid(True)
    target = max(0, expected - 1)
    if target > 0:
        ax.axhline(target, linestyle="--", linewidth=1)
    return target

# ---------- FUNZIONE 1: curva singola ----------
def plot_single(csv_path, expected=20, out_png=None):
    t_s, y = read_csv(csv_path)
    if out_png is None:
        out_png = os.path.splitext(csv_path)[0] + ".png"
    fig, ax = plt.subplots(figsize=(9, 5))
    target = _setup_axes(ax, expected, title=os.path.basename(csv_path))
    ax.step(t_s, y, where="post", linewidth=2)
    ymax = max([target] + y) if y else target
    ax.set_yticks(list(range(0, int(math.ceil(ymax)) + 1)))
    fig.tight_layout(); fig.savefig(out_png, dpi=160)
    print(f"✅ Grafico singolo salvato: {out_png}")
    return out_png

# ---------- FUNZIONE 2: overlay due curve ----------
def plot_overlay(stats_csv, fc_csv, expected=20, out_png=None):
    ts, ys = read_csv(stats_csv)  # Stats/AE
    tf, yf = read_csv(fc_csv)     # First-Contact
    if out_png is None:
        out_png = os.path.splitext(stats_csv)[0] + "-overlay.png"
    fig, ax = plt.subplots(figsize=(9, 5))
    target = _setup_axes(ax, expected, title="Scoperta (ultimo nodo): Stats vs Primo Contatto")
    ax.step(ts, ys, where="post", linewidth=2, label="Stats (AE)")
    ax.step(tf, yf, where="post", linewidth=2, label="Primo contatto (SWIM ∪ Stats)")
    ymax = max([target] + ys + yf) if (ys or yf) else target
    ax.set_yticks(list(range(0, int(math.ceil(ymax)) + 1)))
    ax.legend()
    fig.tight_layout(); fig.savefig(out_png, dpi=160)
    print(f"✅ Grafico overlay salvato: {out_png}")
    return out_png

# ---------- CLI ----------
def main():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(
            "Uso:\n"
            "  python3 plot_ttfd.py single <csv> [EXPECTED_NODES] [output.png]\n"
            "  python3 plot_ttfd.py overlay <stats_csv> <firstcontact_csv> [EXPECTED_NODES] [output.png]\n"
        ); return
    mode = args[0]
    if mode == "single":
        if len(args) < 2: print("Errore: serve <csv>"); return
        csv_path = args[1]
        expected = int(args[2]) if len(args) >= 3 else 20
        out_png  = args[3] if len(args) >= 4 else None
        plot_single(csv_path, expected, out_png)
    elif mode == "overlay":
        if len(args) < 3: print("Errore: servono <stats_csv> <firstcontact_csv>"); return
        stats_csv = args[1]; fc_csv = args[2]
        expected  = int(args[3]) if len(args) >= 4 else 20
        out_png   = args[4] if len(args) >= 5 else None
        plot_overlay(stats_csv, fc_csv, expected, out_png)
    else:
        print("Modo non riconosciuto. Usa 'single' o 'overlay'.")

if __name__ == "__main__":
    main()
