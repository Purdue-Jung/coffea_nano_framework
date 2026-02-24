"""
    Module for applying EGM corrections.
"""
import correctionlib
import yaml
from external.MuonScaRe import pt_resol, pt_scale
from selection.selection_utils import add_to_obj


def muon_sf(obj, sf_name, cfg, pt_field="corr_pt"):
    """
    Apply muon scale factors
    Parameters:
    obj: awkward array
        The muon collection with Pt and Eta attributes
    sf_name: str
        which scale factor to applyf for a complete list see
        https://cms-analysis-corrections.docs.cern.ch/#24cdereprocessingfghiprompt-summer24
    cfg: dict
        Configuration dictionary containing 'data_dir' and 'era' keys
    Returns:
    awkward array of float
        Scale factors for each electron
    """

    if pt_field not in obj.fields:
        pt_field = "pt"

    # Load MUO configuration file
    with open(cfg["data_dir"]+"/Corrections/MUO/muon_Z.yml",
            'r', encoding='utf-8') as f:
        muo_cfg = yaml.safe_load(f)["muon_Z"][cfg["era"]]

    # Load correction set
    muo_corr = correctionlib.CorrectionSet.from_file(muo_cfg["file"])
    muon_sf_ = muo_corr[sf_name]

    if "ID" in sf_name and "Iso" not in sf_name:
        obj["muonIDWeight"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "nominal"
        )
        obj["muonIDWeightSyst_UP"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "systup"
        )
        obj["muonIDWeightSyst_DOWN"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "systdown"
        )
        print("Muon ID SFs obtained.")
    elif "Iso" in sf_name:
        obj["muonIsoWeight"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "nominal"
        )
        obj["muonIsoWeightSyst_UP"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "systup"
        )
        obj["muonIsoWeightSyst_DOWN"] = muon_sf_.evaluate(
            obj.eta, obj[pt_field], "systdown"
        )
        print("Muon Iso SFs obtained.")
    return obj


def muon_corr(events, cfg):
    """
    Apply muon energy scale corrections
    """
    # Load MUO configuration file
    with open(cfg["data_dir"]+"/Corrections/MUO/muon_scalesmearing.yml",
            'r', encoding='utf-8') as f:
        muo_cfg = yaml.safe_load(f)["muon_scalesmearing"][cfg["era"]]

    # Load correction set
    muo_corr = correctionlib.CorrectionSet.from_file(muo_cfg["file"])
    pt = events.Muon.pt
    eta = events.Muon.eta
    phi = events.Muon.phi
    charge = events.Muon.charge
    if cfg["isData"] == "True":
        pt_corr = pt_scale(
            0,
            pt,
            eta,
            phi,
            charge,
            muo_corr,
            nested = True,
        )

    else:
        n_tracker_layers = events.Muon.nTrackerLayers
        event_number = events.event
        luminosity_block = events.luminosityBlock
        pt_corr = pt_scale(
            0,
            pt,
            eta,
            phi,
            charge,
            muo_corr,
            nested = True,
        )

        pt_corr = pt_resol(
            pt_corr,
            eta,
            phi,
            n_tracker_layers,
            event_number,
            luminosity_block,
            muo_corr,
            nested = True
        )

    events = add_to_obj(
        events, "Muon", {"corr_pt": pt_corr}
    )
    return events
