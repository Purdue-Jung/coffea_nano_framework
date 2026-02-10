# Coffea-nano-framework README

Framework for event-wise transformations on NanoAOD data tier format using Coffea and awkward arrays.

## Table of Contents

[Basic Usage](#basic-usage)

- [TTree structure](#ttree-structure)
- [Weights](#weights)
- [Selector script](#selector-script)
- [Running selection](#running-selection)
- [Control histograms](#control-histograms)
- [Trigger](#trigger)
- [Plotting](#plotting)

[Corrections and Scale Factors](#corrections-and-scale-factors)

## Basic Usage

From NanoAOD files, we can apply our event selection to them and create TTrees containing the processed events. To run a basic selection we need three things:

- `/config/selection/tree_structure.yml`: configuration file which decides the structure of the output file, meaning which fields or collections we want to save.
- `/config/selection/weights.yml`: configuration file deciding which weights do we want to apply/create, e.g. `eventWeight`, `trueLevelWeight`, etc. To save this weights, we should refer to `minitree_structure.yml`
- `/selectors/selector.py`: python script describing the whole selection process, follows similar structure to [coffea processors](https://coffea-hep.readthedocs.io/en/latest/notebooks/processing.html#coffea-processors) using awkward arrays.

Also, for further large scale processing, it is necessary to specify the following variables in `main.cfg`:

- `tree_dir`: path where to create selection folders.
- `control_hist_dir`: path where to create control histogram folders.
- `selector_script`: path to `selector.py` with a class called `Selector`.

### TTree Structure

The `.yml` has a structure like

```
tree:
  eventNumber: "eventNumber"
  tau: "Tau."
  met_pt: "MET.pt"
  ...
  <field_tree>: "<field_nanoaod>"
```

`<field_nanoaod>` has to be the exact name in NanoAOD, or the name that you assigned in the selector if it was created during selection. Moreover, you can store entire collections by writing the collection name followed by a dot, as it is shown for `"Tau."`.

**Remark**: Nothing is hard-coded to be save in the tree, only the fields mentioned in `tree_structure.yml` will be save.

### Weights

<span style="color: red;">**Warning (On Development):** weights are not created automatically, the functions (corrections/scalefactors) that create them should be called specifically in the `pre_selection` method.</span>

Weight fields are created before event selection and after pre-selection (object selection) by the framework using the configuration file, which looks like

```
Weights:
  eventWeight:
    - "genWeight"
    - "puWeight"
    - "lep.electronIDWeight"
    - "lbar.electronIDWeight"
    - "lep.muonIDWeight"
    - "lbar.muonIDWeight"
    - "lep.muonIsoWeight"
    - "lbar.muonIsoWeight"
    - "jetsAK4_selected.bShapeWeight"
    - "btagNormWeight"
  trueLevelWeight:
    - "genWeight"
```
where for example, `eventWeight` would be computed as the product of all listed weights.

**Remark:** Even though they are created, they are not automatically saved, check [minitree structure](#minitree-structure)

### Selector Script

The core of the selection code is the selector script which is build by the user, it should have a class which inherits from `processor.SelectionProcessor`, as an example you can check the templates available in [the selectors folder](./selectors/dilepton.py). The framework will run the class method `processor.SelectionProcessor.selection_process`.

To support the object selection, `object_selection` offers some methods to apply cuts in leading and subleading objects or to apply veto maps. Also, `selection_utils` has similar methods worth to look at.

### Running Selection

Once we have our configuration ready, we can either run the selection in a single file using the line command

```
$ python src/run_processor.py --help
usage: run_processor.py [-h] [--output OUTPUT] [--output_histos OUTPUT_HISTOS] [--metadata METADATA] input

Make tree in a slurm job (selection)

positional arguments:
  input                 Input NanoAOD file

options:
  -h, --help            show this help message and exit
  --output OUTPUT       Output tree tag
  --output_histos OUTPUT_HISTOS
                        Output histograms tag
  --metadata METADATA   Metadata file (default: empty)
```

an example of this can be found in [`test_selector.sh`](./scripts/test_selector.sh).

<span style="color: red;">**Warning (On Development)**
For large deployment, we can run `src/make_selection.py` to generate a file `selection_commands.sh`. We can check the available line arguments with 

`python src/make_selection.py --help`
```
usage: make_selection.py [-h] [--metadata METADATA] [--era ERA] [--channels CHANNELS]

Make minitree and event selection

options:
  -h, --help           show this help message and exit
  --metadata METADATA  Metadata file (default: empty)
  --era ERA            Data-taking era (default: empty)
  --channels CHANNELS  Channels to be processed, comma-separated (default: ee,emu,mumu)
```
This script will also generate the folder structure using the argument `CHANNELS`. Then, we can run `selection_commands.sh` using SLURM jobs (check the [Slurm submission information](./readme/Slurm.md) for more info).

### Control Histograms

<span style="color: red;">**Warning:** Not currently implemented, you should create histograms after creating TTrees.</span>

As default, the selector class would generate `cutflow`, `onecut`, and `nminusone` histograms, for more info on these objects check [`coffea.analysis_tools.PackedSelection`](https://coffea-hep.readthedocs.io/en/latest/notebooks/packedselection.html). **Note**: As of `Coffea v2025.3.0`, Weighted Cutflows are not implemented for lazy dask awkward arrays, and probably would never be implemented, so the generated histograms are unweighted for MC. This would probably change once we migrate to virtual awkward arrays.

### Trigger

As of now, the selector loads a configuration file for trigger selection (`/config/selection/HLT.yml`) as `selection.processor.SelectionProcessor.cfg["HLT"]`. Check [$h\to\tau\tau$ selection](../selectors/htautau.py) for current implementation, but it is still Work-In-Progress.

### Plotting
By using `src/make_plotting.py`, basic plots can be generated:

```
$ python src/make_plotting.py -h
usage: make_plotting.py [-h] [-e ERAS] [--do_sub_era] [--file_type FILE_TYPE] [--sample SAMPLE] [--debug] config_file

Make tree in a slurm job (selection)

positional arguments:
  config_file           Path to configuration YAML file.

options:
  -h, --help            show this help message and exit
  -e, --eras ERAS
  --do_sub_era          Whether to do sub-era plots.
  --file_type FILE_TYPE
                        Type of file to process: 'nanoaod', 'trees', 'stacks'.
  --sample SAMPLE       Sample to plot (for not stacks).
  --debug               Whether to run in debug mode (only one file).
```

By changing the `--file_type` option, you can either make individual plots per sample using either NanoAOD files (`nanoaod` option) or the output from your selector (`trees` option). To make stack plots, you can pass the option `stacks`, for which it would process all MC and data.

<span style="color: red;">**Warning:** `trees`, `stacks` not currently implemented.</span>

The configuration file is a YAML file with plot configurations. For an example, you can check `plot_configs/signal_plots.yml`.

## Corrections and Scale Factors

It is possible to either apply central corrections (derived by CMS) or private corrections (derived locally).

### Central corrections

CMS central corrections are documented [here](https://cms-analysis-corrections.docs.cern.ch/#24cdereprocessingfghiprompt-summer24). Configuration files for this type of corrections are stored in `/data/Corrections/<POG>`, where `<POG>` can be
- `BTV`
- `EGM`
- `JME`
- `LUM`
- `MUO`
- `TAU`

Each POG develops their corrections in different ways, therefore `src/corrections/<POG>.py` implements different methods for each correction/scalefactor and POG.

Sometimes POG make their own code for corrections, that is stored under `src/external/*` and if it something big it should be added as a submodule with git. In such case, `./src/corrections/*` represent an interface between our coffea processors (awkward arrays) and their code, please be mindful of that.

### Private corrections

<span style="color: red;">**Warning:** Work-In-Progress not currently implemented.</span>

Private scalefactors computations can be divided in three steps:
- Selection
- Histogramming
- Computation

The first two steps can be handled directly with our selector objects, as an example check `./selectors/njets_sf.py`, here we change `self.output_mode` such that only generates histograms and not minitrees. In this way, the histogramming creation can be easily deplot to cover all datasets.

Once we have histograms, the actual computation can vary, in our example we compute normalization scalefactors as a function of the jet multiplicity to correct b-shape weights, check `./src/sf_correction/histogram_postprocess.py`.

Whatever the computation process is, the final product should be ROOT files storing such scalefactors/corrections, in this way we can apply them using coffea extractors, check `./src/selection/corrections/private.py`.

(Last Edited: January 9, 2026)