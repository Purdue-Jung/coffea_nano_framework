# Slurm Submission

(Edition: January 9, 2026)

<span style="color: red;">**Warning (On Development): This information is focused for Purdue users, it might work for other Slurm-based clusters.**

To submit Slurm jobs we need a list of commands in a `.sh` file. With that we can run `src/common/make_slurm_jobs.py`. Its usage is:

```
usage: MkSlurmJobs.py [-h] [--cpu CPU] [--mem MEM] [--time TIME] [--conda-env CONDA_ENV] [--account ACCOUNT] [--partition PARTITION] [--qos QOS] [--cluster CLUSTER] [--threads THREADS] commandlist

positional arguments:
  commandlist

options:
  -h, --help            show this help message and exit
  --cpu CPU             Number of cpus requested for this job. Default 1.
  --mem MEM             Amount of memory to allocate per cpu for this job. Default 32000.
  --time TIME           Amount of time to submit job for. Default is 4 hours.
  --conda-env CONDA_ENV
                        Conda environment to activate. Default is none.
  --account ACCOUNT     Account to submit job to. Default is cms.
  --partition PARTITION
                        Partition to submit job to. Default is empty.
  --qos QOS             Quality of service to submit job to. Default is empty.
  --cluster CLUSTER     Cluster to submit job to. Default is Hammer.
  --threads THREADS     Number of threads for job creation parallelization.
```

This would create two folders: `<cluster>SlurmJobs` and `<cluster>SlurmOut`, and a submission script `Run<cluster>_<command_list>.sh`. Make sure you are able to open a VOMS proxy in such cluster first.