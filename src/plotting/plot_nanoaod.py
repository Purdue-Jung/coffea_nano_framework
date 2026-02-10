"""
Coffea processor to create histograms and plot them from NanoAOD files.
Not used for Stack Plots, but for individual sample histograms
"""
import json
import os
from coffea import processor
from coffea.nanoevents import NanoAODSchema, NanoEventsFactory
import matplotlib.pyplot as plt
import mplhep as hep
from plotting.hist_processor import HistProcessor
from plotting.plots_constants import COLOR_PALETTE_6

style = hep.style.CMS
style["font.size"] = 20
plt.style.use(style)

def make_plot(histogram, era, config, output_path, args, data=False):
    """Make a plot from a histogram."""
    if len(era.split("-")) > 1:
        lumis = sum(args.lumis[e] for e in era.split("-"))
    else:
        lumis = args.lumis[era]
    fig, ax = plt.subplots(figsize=(12,10))
    hep.cms.label("Work-In-Progress", data=data,
                lumi=f"{lumis/1000:.2f}", ax=ax, com=13.6)
    if 'color' not in config:
        config['color'] = COLOR_PALETTE_6[0]
    hep.histplot(histogram, ax=ax, label=config['label'], color=config['color'])

    if 'xlabel' in config:
        ax.set_xlabel(config['xlabel'])

    if 'ylabel' in config:
        ax.set_ylabel(config['ylabel'])
    else:
        ax.set_ylabel("Events")

    ax.legend([config['title']])
    fig.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)

def make_plotting(args):
    """Make histograms from NanoAOD files."""

    args.fw_dir = args.main_config['fw_dir']

    with open(args.fw_dir + "/config/datasets/Nominal.json", encoding='utf-8') as dataset_file:
        datasets = json.load(dataset_file)

    fileset = {}
    for era in datasets:
        for sample in datasets[era]:
            if args.sample != "" and sample != args.sample:
                continue
            fileset[era + ";" + sample] = {
                "files": {
                    f.replace("/eos/global/", "root://cms-xrd-global.cern.ch//"): "Events"
                    for f in datasets[era][sample]["files"]
                },
                "metadata": {
                    "isMC": not "run" in sample,
                }
            }

    if args.debug:
        proc = HistProcessor(args, args.cfg, "_", mode="virtual")
        sample_name = next(iter(fileset))
        files = fileset[sample_name]["files"]
        filename = next(iter(files))
        metadata = fileset[sample_name]["metadata"]
        metadata["dataset"] = sample_name
        events = NanoEventsFactory.from_root(
            {filename: "Events"},
            schemaclass=NanoAODSchema,
            metadata=metadata
        ).events()
        out = proc.process(events)
        print(out)
        raise NotImplementedError("Debug mode, stopping after processing one file.")

    futures_run = processor.Runner(
        executor=processor.FuturesExecutor(workers=16, compression=None),
        schema=NanoAODSchema,
        savemetrics=True,
    )
    out, _ = futures_run(
        fileset,
        processor_instance=HistProcessor(args, args.cfg, "_", mode="virtual"),
    )
    # We assume that args.cfg is a dict with the histogram configurations,
    # step is left as "_" since it's not relevant for plotting at the nanoaod level.
    for sample in out:
        era, sample_name = sample.split(";")
        for histo_name, histo in out[sample].items():
            config = args.cfg[histo_name]
            output_path = (f"{args.main_config['plot_dir']}/nanoaod"
                            f"/{era}/{sample_name}/{histo_name}.pdf")
            if not os.path.exists(os.path.dirname(output_path)):
                os.makedirs(os.path.dirname(output_path))

            # Lumi weight
            lumi_weight = args.lumis[era] * args.xsecs[sample_name] / histo.sum().value
            histo = histo * lumi_weight
            if not 'title' in config:
                config['title'] = f"{sample_name}"
            make_plot(histo, era, config, output_path, args, data="run" in sample_name)
