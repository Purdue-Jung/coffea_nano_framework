"""
Coffea processor for multiple histogram making.
For plotting purposes.
"""
import hist
from coffea import processor
import awkward as ak
from uncertainties import ufloat
import common.utils
from common.variables import get_variable

class HistProcessor(processor.ProcessorABC):
    """Processor to create histograms from events."""
    def __init__(self, args, step_histos, step, mode="virtual"):
        """Initialize the processor."""
        assert mode in ["eager", "virtual", "dask"]
        self._mode = mode
        self.args = args
        self.step_histos = step_histos
        self.step = step

    def process(self, events):
        """Process the events and fill histograms."""
        histos = {}
        for histo, histo_config in self.step_histos.items():
            hist_axis = None
            match histo_config['axis_type']:
                case 'Regular':
                    if 'inputs' in histo_config:
                        hist_axis = hist.axis.Regular(
                            *histo_config["inputs"],
                            name=histo_config['field'], label=histo_config['label']
                        )
                    else:
                        hist_axis = hist.axis.Regular(
                            histo_config["nbins"], histo_config["xmin"], histo_config["xmax"],
                            name=histo_config['field'], label=histo_config['label']
                        )
                case _:
                    raise ValueError(f"Axis type {histo_config['axis_type']} not recognized.")

            hh_subproc = hist.Hist(hist_axis, storage=hist.storage.Weight())
            weight_arr = self.get(events,histo_config['weights'][0])
            if len(histo_config['weights']) > 1:
                for w in histo_config['weights'][1:]:
                    new_weight = self.get(events,w)
                    if new_weight.layout.minmax_depth != (1,1):
                        new_weight = ak.prod(new_weight, axis=1)
                    weight_arr = weight_arr * new_weight
            if "reject_weights" in histo_config:
                for rw in histo_config['reject_weights']:
                    reject_weight = self.get(events,rw)
                    if reject_weight.layout.minmax_depth != (1,1):
                        reject_weight = ak.prod(reject_weight, axis=1)
                    weight_arr = weight_arr / reject_weight

            fields = histo_config['field'].split(",")
            for field in fields:
                var = self.get(events,field,sub_idx=histo_config.get("subIdx", None))

                if events.metadata["isMC"]:
                    if var.layout.minmax_depth != (1,1):
                        new_weights = weight_arr*ak.ones_like(var, dtype=float)
                        var = ak.flatten(var)
                        weight_arr = ak.flatten(new_weights)
                    nan_mask = ak.is_none(var)
                    var = var[~nan_mask]
                    weight_arr = weight_arr[~nan_mask]
                    hh_subproc.fill(var, weight=weight_arr)
                else:
                    if var.layout.minmax_depth != (1,1):
                        var = ak.flatten(var)
                    nan_mask = ak.is_none(var)
                    var = var[~nan_mask]
                    hh_subproc.fill(var)

            if 'merge_overflow' in histo_config and not histo_config['merge_overflow']:
                pass
            else:
                overflow = hh_subproc[hist.overflow]
                if overflow.value != 0:
                    hh_uarray = common.utils.convert_hist_to_uarray(hh_subproc)
                    overflow = ufloat(overflow.value, overflow.variance**0.5)
                    hh_uarray[-1] = hh_uarray[-1] + overflow
                    hh_subproc = common.utils.convert_uarray_to_hist(hh_subproc, hh_uarray)

            if 'merge_underflow' in histo_config and not histo_config['merge_underflow']:
                pass
            else:
                underflow = hh_subproc[hist.underflow]
                if underflow.value != 0:
                    hh_uarray = common.utils.convert_hist_to_uarray(hh_subproc)
                    underflow = ufloat(underflow.value, underflow.variance**0.5)
                    hh_uarray[0] = hh_uarray[0] + underflow
                    hh_subproc = common.utils.convert_uarray_to_hist(hh_subproc, hh_uarray)

            histos[histo] = hh_subproc
        dataset = events.metadata["dataset"]
        return {dataset: histos}

    def postprocess(self, accumulator):
        """Postprocess the histograms. Not used in this case."""

    def get(self, events, field, sub_idx=None):
        """Get a field from the events, handling nested fields."""
        if "." in field:
            parts = field.split(".")
            if parts[0] not in events.fields:
                raise ValueError(f"Field {parts[0]} not found in events.")
            var = events[parts[0]]
            if sub_idx is not None and sub_idx >= 0:
                try:
                    var = ak.pad_none(var, sub_idx+1)
                    var = var[:,int(sub_idx)]
                except Exception as e:
                    print(f"Error applying sub_idx {sub_idx} to variable {parts[0]}: {e}")
                    print(f"Variable structure: {var}")
                    raise e
            for part in parts[1:]:
                if part not in var.fields:
                    raise ValueError(f"Field {part} not found in {var}.")
                var = var[part]
            return var
        else:
            try:
                return events[field]
            except ak.errors.FieldNotFoundError:
                return get_variable(events, field) # try to compute variable from metadata
