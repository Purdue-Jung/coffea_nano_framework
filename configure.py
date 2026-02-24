"""
CLI configuration for the framework, it would modify main.cfg.
"""
import os
import argparse
import json

default_parameters = {
    # Analysis parameters
    "processes": "RunIII",
    "systematics": "RunIII",
    # Framework paths
    "plot_dir": "${fw_dir}/plots",
    ## tree creation
    "selector": "htautau",
    # Other parameters
    "signals": "VBF_Hto2Tau"
}

def get_enumerated_option(options_path, file_extension):
    """Get a list of options from a directory and enumerate them."""
    options = [
        f.split(file_extension)[0] for f in os.listdir(options_path) if f.endswith(file_extension)]
    for i, option in enumerate(options):
        print(f" {i}: {option}")
    return options

def ask_parameters(fw_dir):
    """Ask the user for configuration parameters"""
    parameters = {}
    print("Please provide the following configuration parameters (press Enter to use default):")

    # Processes
    print("\nAvailable processes:")
    processes_options = get_enumerated_option(f"{fw_dir}/config/processes/", ".json")
    processes_input = input(
        f"Processes to run (default: {default_parameters['processes']}): ").strip()
    parameters['processes'] = processes_options[int(processes_input)] \
        if processes_input else default_parameters['processes']

    # Systematics
    print("Available systematics:")
    systematics_options = get_enumerated_option(f"{fw_dir}/config/systematics/", ".json")
    systematics_input = input("Systematics to run "
                            f"(default: {default_parameters['systematics']}): ").strip()
    parameters['systematics'] = [systematics_options[int(systematics_input)]] \
        if systematics_input else default_parameters['systematics']

    print("\nNow defining paths...\nUse <fw_dir> to refer to the framework directory.")

    # Plot directory
    plot_dir_input = input(f"Plot directory (default: {default_parameters['plot_dir']}): ").strip()
    parameters['plot_dir'] = plot_dir_input if plot_dir_input else default_parameters['plot_dir']

    # tree creation
    tree_creation = input(
        "Do you want to configure tree (selection) creation parameters? (Y/N, default: N): "
        ).strip().upper()
    if tree_creation == 'Y':
        print("Available selectors:")
        selector_options = get_enumerated_option(f"{fw_dir}/selectors/", ".py")
        selector_input = input(f"Selector script (default: "
            f"{default_parameters['selector']}): ").strip()
        parameters['selector'] = selector_options[int(selector_input)] \
            if selector_input else default_parameters['selector']
    else:
        parameters['selector'] = default_parameters['selector']

    # Other parameters
    print("\nAnalysis parameters:")
    print("Select signal processes (press Enter to use default and stop listing).")
    print("Available processes:")
    with open(f"{fw_dir}/config/processes/{parameters['processes']}.json",
                "r", encoding="utf-8") as f:
        processes_config = json.load(f)
        processes_list = list(processes_config.keys())
    for i, proc in enumerate(processes_list):
        print(f" {i}: {proc}")
        print("Subprocesses:", ", ".join(processes_config[proc]))
    first_signal = input("Include signal: ").strip()
    signals = []
    while first_signal:
        signals.append(processes_list[int(first_signal)])
        first_signal = input("Include another signal (or press Enter to stop): ").strip()
    parameters['signals'] = ",".join(signals) if signals else default_parameters['signals']

    parameters['fw_dir'] = fw_dir

    return parameters


def fill_cfg(parameters):
    """Fill the main.cfg file with the given parameters"""
    fw_dir = parameters.get('fw_dir', os.getcwd())
    cfg_path = os.path.join(fw_dir, "main.cfg")

    cfg_text = ""
    cfg_text += "###################################################################\n"
    cfg_text += "#################### FRAMEWORK CONFIGURATION ####################\n"
    cfg_text += "###################################################################\n\n"

    cfg_text += "########## Processes and systematics to run ##########\n"
    cfg_text += f"processes = {parameters.get('processes', '')}\n"
    cfg_text += f"systematics = {parameters.get('systematics', '')}\n\n"

    cfg_text += "########## Paths to directories and scripts ##########\n"
    cfg_text += "# Framework directory\n"
    cfg_text += f"fw_dir = {fw_dir}\n"
    # cfg_text += "# EOS directory\n"
    # cfg_text += f"eos_dir = {parameters.get('eos_dir', '')}\n"
    cfg_text += "# Where to save plots\n"
    cfg_text += f"plot_dir = {parameters.get('plot_dir', '').replace('<fw_dir>', fw_dir)}\n\n"

    cfg_text += "## Tree creation\n"
    cfg_text += "# Selector script for minitree creation\n"
    cfg_text += f"selector = {parameters.get('selector', '')}\n"
    cfg_text += "# Where to save trees\n"
    cfg_text += f"tree_dir = {parameters.get('tree_dir', '').replace('<fw_dir>', fw_dir)}\n"
    cfg_text += "# Where to save control histograms\n"
    cfg_text += ("control_hist_dir = "
                f"{parameters.get('control_hist_dir', '').replace('<fw_dir>', fw_dir)}\n\n")

    cfg_text += "########## Other parameters ##########\n"
    cfg_text += f"signals = {parameters.get('signals', '')}\n"

    with open(cfg_path, "w", encoding="utf-8") as cfg_file:
        cfg_file.write(cfg_text)
    print(f"Configuration file created at {cfg_path}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Configure the framework settings.")
    parser.add_argument('-d', '--default', action='store_true',
                        help='Use default configuration parameters')
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_arguments()

    pwd = os.getcwd()
    if 'coffea_nano_framework' == pwd.split('/')[-1]:
        fw_dir = pwd
    elif 'coffea_nano_framework' in os.listdir(pwd):
        fw_dir = os.path.join(pwd, 'coffea_nano_framework')
    else:
        raise RuntimeError(
            "Please run the script from the framework directory or its parent directory.")

    if args.default:
        parameters = default_parameters.copy()
        parameters['fw_dir'] = fw_dir
    else:
        parameters = ask_parameters(fw_dir)

    # eos_uname = input("Enter your EOS username (or press Enter to skip): ").strip()
    # if not eos_uname:
    #     print("No EOS username provided, skipping EOS-specific configuration.")
    #     print("Any code depending on EOS could fail.")
    #     parameters["eos_dir"] = ""
    # else:
    #     parameters["eos_dir"] = f"/eos/purdue/store/user/{eos_uname}"

    fill_cfg(parameters)

if __name__ == "__main__":
    main()
