"""
Make SLURM job scripts for running commands in a CMS environment.
This script generates SLURM job scripts for executing commands in a CMS environment,
handling the setup of the environment, job parameters, and command execution.
"""
#!/usr/bin/env python
import argparse
import os
import re
import stat
import concurrent.futures
import common.utils

def get_el_version():
    """Get the major version of the Enterprise Linux (EL) distribution."""
    # Try reading /etc/os-release first
    try:
        with open('/etc/os-release', encoding='utf-8') as f:
            for line in f:
                if line.startswith("VERSION_ID"):
                    # VERSION_ID might be quoted, so strip quotes and whitespace.
                    version_id = line.split("=")[1].strip().strip('"')
                    # In case there is a minor version (e.g., "8.4"), take only the major number.
                    major_version = version_id.split('.')[0]
                    return int(major_version)
    except (FileNotFoundError, PermissionError) as e:
        print(f"Warning: Cannot read /etc/os-release: {e}")

    # Fallback: Try reading /etc/redhat-release
    try:
        with open('/etc/redhat-release', encoding='utf-8') as f:
            content = f.read()
            # Example string: "Red Hat Enterprise Linux release 8.4 (Ootpa)"
            match = re.search(r'release\s+(\d+)', content, re.IGNORECASE)
            if match:
                return int(match.group(1))
    except (FileNotFoundError, PermissionError) as e:
        print(f"Warning: Cannot read /etc/redhat-release: {e}")

    return None


def write_common_commands(f, cmssw_base, command, args):
    """
    Write the shared environment setup commands to the file-like object `f`.
    """
    # Change directory to the CMSSW src directory.
    # f.write("cd " + cmssw_base + "/src/ || exit 1\n")
    f.write("cd " + cmssw_base + "|| exit 1\n")
    # purge any existing modules
    # f.write("module --force purge\n")
    f.write("source /etc/profile.d/modules.sh\n")
    # Source the OSG WN client for EL8.
    if len(args.conda_env) > 0:
        if args.cluster == "Hammer":
            f.write("module load anaconda\n")
        elif args.cluster == "Gautschi":
            f.write("module load conda\n")
        else:
            raise NotImplementedError(f"Cluster {args.cluster} "
                                        "is not supported for conda environment activation.")
        f.write(f"conda activate {args.conda_env}\n")
    else:
        f.write("source /cvmfs/oasis.opensciencegrid.org/osg-software/"
                "osg-wn-client/current/el8-x86_64/setup.sh\n")
        # Set up the proxy.
        f.write("export X509_USER_PROXY=~/x509up_u$(id -u)\n")
        # f.write("voms-proxy-init -voms cms -valid 999:00:00\n")
        # Source the CMS environment.
        f.write("source /cvmfs/cms.cern.ch/cmsset_default.sh\n")
        f.write("export BOOST_ROOT=/cvmfs/cms.cern.ch/"
                "slc7_amd64_gcc700/external/boost/1.63.0-gnimlf\n")
        f.write("export SCRAM_ARCH=el8_amd64_gcc12\n")
        # Add the directory with the extra libraries.
        f.write("export LD_LIBRARY_PATH=/external_libs:$LD_LIBRARY_PATH\n")
        # Change to the analysis directory.
        # f.write("cd TopAnalysis/Configuration/analysis/diLeptonic || exit 1\n")
        # activate conda environment if specified
        # if len(args.conda_env) > 0:
        #         f.write("module load anaconda/2020.11-py38\n")
        #         f.write(f"conda activate {args.conda_env}\n")
        f.write("eval \"$(scramv1 runtime -sh)\"\n")

    if len(args.fw_dir) > 0:
        f.write(f"cd {args.fw_dir}\n")
        f.write(f"export PYTHONPATH='{args.fw_dir}/src:$PYTHONPATH'\n")
    # Echo the command for logging (escaping any double quotes).
    f.write('echo "' + command.replace('"', '\\"') + '"\n')
    # Execute the command.
    if ';' in command:
        for cmd in command.split(";"):
            f.write(cmd + "\n")
    else:
        f.write(command + "\n")

def process_job(job_id, command, args, cmssw_base):
    """
    Write the two scripts (the in-container script and the SLURM job script)
    for a given job and return its ID and job script filename.

    Parameters:
    job_id (int): The job ID.
    command (str): The command to execute
    args (argparse.Namespace): The parsed command-line arguments.
    cmssw_base (str): The CMSSW base directory.

    Returns:
    Tuple[int, str]: The job ID and the job script filename.
    """
    # Build filenames for the job script and the in-container script.
    job_script = f"{args.cluster}{args.account}SlurmJobs/SlurmJob_{job_id}.sh"
    in_container_script = f"{args.cluster}{args.account}SlurmJobs/InContainer_{job_id}.sh"

    el_version = get_el_version()

    if el_version != 8:
        # Write the in-container script.
        with open(in_container_script, "w", encoding='utf-8') as incfg:
            write_common_commands(incfg, cmssw_base, command, args)

        # Make the in-container script executable.
        os.chmod(in_container_script, stat.S_IRWXU)

    # Write the SLURM job submission script.
    with open(job_script, "w", encoding='utf-8') as cfg:
        cfg.write("#!/bin/sh\n")
        cfg.write(f"#SBATCH  -A {args.account}\n")
        cfg.write("#SBATCH --ntasks=1\n")
        cfg.write("#SBATCH --cpus-per-task=" + str(args.cpu) + "\n")
        cfg.write("#SBATCH --mem-per-cpu=" + str(args.mem) + "\n")
        cfg.write("#SBATCH --time=" + str(args.time) + "\n")
        if len(args.partition) > 0:
            cfg.write("#SBATCH --partition=" + args.partition + "\n")
        if len(args.qos) > 0:
            cfg.write("#SBATCH --qos " + args.qos + "\n")
        cfg.write(
            "#SBATCH --output=" + f"{args.cluster}{args.account}SlurmOut/slurm-{job_id}-%j.out\n")
        # Call the CMS EL8 container wrapper to run the in-container script.
        if el_version == 8:
            write_common_commands(cfg, cmssw_base, command, args)
        elif args.cluster == 'Gautschi':
            cfg.write('module --force unload xalt \n')
            cfg.write('/cvmfs/cms.cern.ch/common/cmssw-el8 '
                      '-B /cvmfs -B /depot/cms/top/awildrid/compat_libs/usr/lib64:/external_libs '
                      f'-- /bin/bash {in_container_script}\n')
        else:
            cfg.write('/cvmfs/cms.cern.ch/common/cmssw-el8 '
                      '-B /cvmfs -B /depot/cms/top/awildrid/compat_libs/usr/lib64:/external_libs '
                      f'-- /bin/bash {in_container_script}\n')
    return job_id, job_script

def argparser():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("commandlist")
    parser.add_argument("--cpu", type=str, default="1",
                        help='Number of cpus requested for this job. Default 1.')
    parser.add_argument("--mem", type=str, default="32000",
                        help='Amount of memory to allocate per cpu for this job. Default 32000.')
    parser.add_argument("--time", type=str, default="1-00:00:00",
                        help='Amount of time to submit job for. Default is 4 hours.')
    parser.add_argument("--conda-env", type=str, default="",
                        help='Conda environment to activate. Default is none.')
    parser.add_argument("--account", type=str, default="cms",
                        help='Account to submit job to. Default is cms.')
    parser.add_argument("--partition", type=str, default="",
                        help='Partition to submit job to. Default is empty.')
    parser.add_argument("--qos", type=str, default="",
                        help='Quality of service to submit job to. Default is empty.')
    parser.add_argument("--cluster", type=str, default="Hammer",
                        help='Cluster to submit job to. Default is Hammer.')
    parser.add_argument("--threads", type=int, default=20,
                        help='Number of threads for job creation parallelization.')
    args = parser.parse_args()
    return args

def main():
    """Main function to execute the script."""
    args = argparser()
    args.fw_dir = common.utils.parse_main_config()["fw_dir"]

    # # Ensure that cmssw_base is defined.
    # cmssw_base = os.getenv("cmssw_base")
    # if not cmssw_base:
    #     raise RuntimeError("Environment variable cmssw_base is not set.")

    # better than using cmssw_base
    cwd = os.path.abspath(__file__)
    dileptonic_dir = os.path.dirname(cwd)
    cmssw_base = dileptonic_dir

    # Create the required directories (if they don't already exist).
    os.makedirs(f"{args.cluster}{args.account}SlurmJobs", exist_ok=True)
    os.makedirs("nohuplogs", exist_ok=True)
    os.makedirs("plotslogs", exist_ok=True)
    os.makedirs(f"{args.cluster}{args.account}SlurmOut/", exist_ok=True)

    # Determine starting index from any existing job scripts.
    i = 0
    for job in os.listdir(f"{args.cluster}{args.account}SlurmJobs"):
        if job.startswith("SlurmJob_") and job.endswith(".sh"):
            try:
                num = int(job[len("SlurmJob_"):-len(".sh")])
                if num >= i:
                    i = num + 1
            except ValueError:
                pass

    # We'll use 'start_index' as the base for new job IDs.
    start_index = i

    # Read the command list file and filter out empty lines and comments.
    with open(args.commandlist, "r", encoding='utf-8') as commandlistfile:
        all_lines = commandlistfile.readlines()
    commands = [line.strip() for line in all_lines
                if line.strip() and not line.strip().startswith("#")]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = []
        for idx, command in enumerate(commands):
            # Mimic the original behavior (i += 1 for each valid command).
            job_id = start_index + idx + 1
            futures.append(executor.submit(process_job, job_id, command, args, cmssw_base))
        # Gather the results as they complete.
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except concurrent.futures.CancelledError as ce:
                print("Job was cancelled:", ce)
            except (OSError, ValueError) as e:
                print("Unexpected error processing a job:", e)

    # Sort the results by job ID to preserve order.
    results.sort(key=lambda x: x[0])
    el_version = get_el_version()

    # Create a run script that will sbatch all the job scripts,
    # separate on maximum 5000 jobs per script.
    if len(results) > 5000:
        # Split the results into chunks of 5000 jobs.
        chunk_size = 5000
        for i in range(0, len(results), chunk_size):
            chunk = results[i:i + chunk_size]
            runfile_basename = (f"Run{args.cluster}{args.account}Slurm_"
                                f"{os.path.basename(args.commandlist).rsplit('.', 1)[0]}_"
                                f"{i // chunk_size}.sh")
            with open(runfile_basename, "w", encoding='utf-8') as runfile:
                runfile.write("#!/bin/sh\n")
                match el_version:
                    case 7:
                        runfile.write('source '
                                      '/cvmfs/oasis.opensciencegrid.org/osg-software/'
                                      'osg-wn-client/3.6/3.6.240627-1/el7-x86_64/setup.sh\n')
                    case 8:
                        runfile.write("source "
                                      "/cvmfs/oasis.opensciencegrid.org/osg-software/"
                                      "osg-wn-client/current/el8-x86_64/setup.sh\n")
                    case 9:
                        runfile.write("source "
                                      "/cvmfs/oasis.opensciencegrid.org/osg-software/"
                                      "osg-wn-client/current/el9_x86_64/setup.sh\n")
                    case _:
                        raise RuntimeError("Unsupported EL version.")
                runfile.write("export X509_USER_PROXY=~/x509up_u`id -u`\n")
                runfile.write("voms-proxy-init -voms cms -valid 999:00:00\n")
                # Write an sbatch command for each job in the chunk.
                for job_id, job_script in chunk:
                    runfile.write("sbatch " + job_script + "\n")
    else:
        runfile_basename = (f"Run{args.cluster}{args.account}Slurm_"
                            f"{os.path.basename(args.commandlist).rsplit('.', 1)[0]}.sh")
        with open(runfile_basename, "w", encoding='utf-8') as runfile:
            runfile.write("#!/bin/sh\n")
            match el_version:
                case 7:
                    runfile.write('source /cvmfs/oasis.opensciencegrid.org/osg-software/'
                                  'osg-wn-client/3.6/3.6.240627-1/el7-x86_64/setup.sh\n')
                case 8:
                    runfile.write("source /cvmfs/oasis.opensciencegrid.org/osg-software/"
                                  "osg-wn-client/current/el8-x86_64/setup.sh\n")
                case 9:
                    runfile.write("source /cvmfs/oasis.opensciencegrid.org/osg-software/"
                                  "osg-wn-client/current/el9_x86_64/setup.sh\n")
                case _:
                    raise RuntimeError("Unsupported EL version.")
            runfile.write("export X509_USER_PROXY=~/x509up_u`id -u`\n")
            runfile.write("voms-proxy-init -voms cms -valid 999:00:00\n")
            # Write an sbatch command for each job.
            for job_id, job_script in results:
                runfile.write("sbatch " + job_script + "\n")

    print(f"Writed submission script on {runfile_basename}")

if __name__ == "__main__":
    main()
