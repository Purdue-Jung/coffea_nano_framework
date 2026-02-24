"""
Module for producing configuration files from a file with DAS dataset definitions.
"""
import subprocess
import argparse
import yaml
import json
import common.utils


def process_datasets(das_datasets):
    """Process DAS datasets and produce configuration files."""
    output = {}
    eos_prefix = "/eos/global" # by default use global EOS
    print("Processing Data DAS datasets...")
    for year, datasets in das_datasets.items():
        output[year] = {}
        for subproc_name, dataset_queries in datasets.items():
            nqueries = len(dataset_queries)
            eos_paths = []
            sizes = []
            nevents = []
            for i, dataset_query in enumerate(dataset_queries, start=1):
                print(f"Processing {subproc_name} {i}/{nqueries}: {dataset_query}", end="\r")
                if not dataset_query.startswith("/"):
                    print(f"Skipping invalid dataset query: {dataset_query}")
                    continue
                command = ['/cvmfs/cms.cern.ch/common/dasgoclient', "--query",
                           f'file dataset={dataset_query}', "-json"]
                try:
                    result = subprocess.run(
                                command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                check=True
                            )
                except subprocess.CalledProcessError as e:
                    print(f"Error querying DAS for dataset {dataset_query}: {e.stderr}")
                    raise e
                result = json.loads(result.stdout)  # Remove [ and ] from JSON array
                if not 'file' in result[0]:
                    raise ValueError(
                        f"Unexpected DAS output for dataset {dataset_query}: {result.stdout}"
                    )
                for fileinfo in result:
                    filepath = fileinfo['file'][0]['name']
                    size = fileinfo['file'][0]['size']
                    nevt = fileinfo['file'][0]['nevents']
                    eos_paths.append(f"{eos_prefix}{filepath}")
                    sizes.append(size)
                    nevents.append(nevt)
                print("",end="\r")
            if not eos_paths:
                print(f"No valid EOS paths found for {subproc_name}, skipping.")
                continue
            output[year][subproc_name] = {"files": eos_paths,
                                           "sizes": sizes,
                                           "nevents": nevents}
    return output

def process_mc(das_datasets):
    """Process MC DAS datasets and produce configuration files."""
    output = {}
    eos_prefix = "/eos/global"
    print("Processing MC DAS datasets...")
    for subproc_name, per_year_datasets in das_datasets.items():
        subproc_str = {subproc_name: {}}
        for year, dataset_queries in per_year_datasets.items():
            if year not in output:
                output[year] = {}
            eos_paths = []
            sizes = []
            nevents = []
            nqueries = len(dataset_queries)
            for i, dataset_query in enumerate(dataset_queries, start=1):
                print(f"Processing {subproc_name} {year} {i}/{nqueries}: {dataset_query}", end="\r")
                if not dataset_query.startswith("/"):
                    print(f"Skipping invalid dataset query: {dataset_query}")
                    continue
                command = ['/cvmfs/cms.cern.ch/common/dasgoclient', "--query",
                           f'file dataset={dataset_query}', "-json"]
                result = subprocess.run(
                            command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=True
                        )
                result = json.loads(result.stdout)  # Remove [ and ] from JSON array
                if len(result) == 0:
                    print(f"No files found for dataset {dataset_query}, skipping.")
                    print(result)
                    raise ValueError(
                        f"No files found for dataset {dataset_query}, skipping."
                    )
                if not 'file' in result[0]:
                    raise ValueError(
                        f"Unexpected DAS output for dataset {dataset_query}: {result.stdout}"
                    )
                for fileinfo in result:
                    filepath = fileinfo['file'][0]['name']
                    size = fileinfo['file'][0]['size']
                    nevt = fileinfo['file'][0]['nevents']
                    eos_paths.append(f"{eos_prefix}{filepath}")
                    sizes.append(size)
                    nevents.append(nevt)
                print("",end="\r")
            if not eos_paths:
                print(f"No valid EOS paths found for {subproc_name} in {year}, skipping.")
                continue
            subproc_str[subproc_name][year] = {"files": eos_paths,
                                                "sizes": sizes,
                                                "nevents": nevents}
        if not subproc_str[subproc_name]:
            print("No valid years found for "f"{subproc_name}, skipping.")
            continue  # Skip if no valid years were added
        for year in subproc_str[subproc_name]:
            output[year][subproc_name] = subproc_str[subproc_name][year]
    return output

def argsparser():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Produce NTuples datasets configuration from DAS dataset definitions."
    )
    parser.add_argument(
        "input",
        help="Path to the input YAML file containing DAS dataset definitions.",
    )
    return parser.parse_args()

def main():
    """Main function"""
    args = argsparser()

    args.main_config, _, _ = common.utils.initial_loading()

    with open(args.input, "r", encoding="utf-8") as file:
        das_datasets = yaml.safe_load(file)

    output = {}
    # Data processing
    data_output = process_datasets(das_datasets["Data"])
    for year, data_per_proc in data_output.items():
        if year not in output:
            output[year] = {}
        output[year].update(data_per_proc)

    # MC processing
    mc_output = process_mc(das_datasets["MC"])
    for year, mc_per_proc in mc_output.items():
        if year not in output:
            output[year] = {}
        output[year].update(mc_per_proc)

    with open(args.main_config["fw_dir"]+'/config/datasets/Nominal.json',
            "w", encoding="utf-8") as outfile:
        json.dump(output, outfile, indent=4)

    print("NTuples datasets configuration has been written to "
        f"{args.main_config['fw_dir']}/config/datasets/Nominal.json")

if __name__ == "__main__":
    main()
