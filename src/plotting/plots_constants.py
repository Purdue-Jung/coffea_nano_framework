"""
    Constants and methods for plotting
"""


COLOR_PALETTE_6 = ["#e42536", "#f89c20", "#5790fc",
                   "#964a8b", "#9c9ca1", "#7a21dd"]
COLOR_PALETTE_8 = ["#c91f16", "#ff5e02", "#1845fb", "#c849a9",
                   "#adad7d", "#86c8dd", "#578dff", "#656364"]
COLOR_PALETTE_10 = ["#bd1f01", "#ffa90e", "#3f90da", "#94a4a2", "#832db6",
                    "#a96b59", "#e76300", "#b9ac70", "#717581", "#92dadd"]

PROCESS_LABELS = {
    "ttbar": r"$t\bar{t}$",
    "Z+jets": "Z+Jets",
    "top": "Single t",
    "diboson": "Diboson"
}

CHAN_LABELS = {
    "ee": r"$ee$",
    "mumu": r"$\mu\mu$",
    "emu": r"$e\mu$",
    "etau": r"$e\tau$",
    "mutau": r"$\mu\tau$",
    "tautau": r"$\tau\tau$",
    "combined": r"$e\tau + \mu\tau + \tau\tau$"
}

def get_color_palette(n_colors):
    """Get a color palette with the specified number of colors."""
    if n_colors <= 6:
        return COLOR_PALETTE_6[:n_colors]
    elif n_colors <= 8:
        return COLOR_PALETTE_8[:n_colors]
    elif n_colors <= 10:
        return COLOR_PALETTE_10[:n_colors]
    else:
        raise ValueError("Color palette supports up to 10 colors only.")