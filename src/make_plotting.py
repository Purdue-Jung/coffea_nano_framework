"""
Run the plotting scripts to make histograms either in NanoAOD or output trees.
"""
import argparse
import json
import yaml
import common.utils
from plotting.plot_nanoaod import make_plotting as make_plotting_nanoaod
# from plotting.plot_trees import make_plotting as make_plotting_trees
# from plotting.plot_stacks import make_plotting as make_plotting_stacks

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Make tree in a slurm job (selection)")
    parser.add_argument("config_file", type=str, help="Path to configuration YAML file.")
    parser.add_argument("-e", "--eras", type=str, default="")
    parser.add_argument("--do_sub_era", action="store_true",
                        help="Whether to do sub-era plots.")
    parser.add_argument("--file_type", type=str, default="nanoaod",
                        help="Type of file to process: 'nanoaod', 'trees' or 'stacks'.")
    parser.add_argument("--sample", type=str, default="", help="Sample to plot (for not stacks).")
    parser.add_argument("--debug", action="store_true",
                        help="Whether to run in debug mode (only one file).")
    return parser.parse_args()

def main(config_file=None, eras="", do_sub_era=False, file_type="nanoaod", sample="") -> None:
    """Main function to run the plotting processor."""
    if config_file is None:
        args = parse_args()
    else:
        args = argparse.Namespace(config_file=config_file, eras=eras,
                                do_sub_era=do_sub_era, file_type=file_type, sample=sample,
                                debug=False)

    with open(args.config_file, "r", encoding="utf-8") as f:
        args.cfg = yaml.safe_load(f)

    args.main_config, args.processes, args.systematics = common.utils.initial_loading()

    if args.eras == "":
        args.eras = args.main_config['eras']
    else:
        args.eras = args.eras.split(",")


    with open(f"{args.main_config['fw_dir']}/data/LumiWeight/luminosity.json",
            "r", encoding='utf-8') as f:
        args.lumis = json.load(f)

    with open(f"{args.main_config['fw_dir']}/data/LumiWeight/cross_sections.yml",
        "r", encoding='utf-8') as f:
        args.xsecs = yaml.safe_load(f)["CrossSections"]

    match args.file_type:
        case "nanoaod":
            make_plotting_nanoaod(args)
        case "trees":
            # make_plotting_trees(args)
            raise NotImplementedError("Tree plotting not implemented yet.")
        case "stacks":
            # make_plotting_stacks(args)
            raise NotImplementedError("Stack plotting not implemented yet.")
        case _:
            raise ValueError(f"File type {args.file_type} not recognized.")


if __name__ == "__main__":
    main()
