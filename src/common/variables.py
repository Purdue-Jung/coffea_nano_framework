"""
This module defines analysis variables
"""
import awkward as ak
import numpy as np

def get_variable(events, variable_name, object_name=None):
    """Get the variable from the events metadata."""
    available_variables = {
        "mjj": compute_mjj, # type: ignore
        "deltaEtajj": compute_delta_etajj, # type: ignore
        "deltaRjj": compute_delta_rjj, # type: ignore
        "deltaPhijj": compute_delta_phijj, # type: ignore
        "mT": compute_mt,
    }
    if variable_name not in available_variables:
        raise ValueError(f"Variable {variable_name} is not defined.")
    return available_variables[variable_name](events, object_name=object_name)

def compute_mjj(events, object_name=None):
    """Compute the invariant mass of the two leading jets."""
    jets = events[object_name] if object_name is not None else events.Jet
    jets = ak.pad_none(jets, 2) # Ensure we have at least 2 jets, pad with None if not
    jet1 = jets[:,0]
    jet2 = jets[:,1]
    mjj = (jet1 + jet2).mass
    mjj = ak.fill_none(mjj, -999)
    return mjj

def compute_delta_etajj(events, object_name=None):
    """Compute the delta eta between the two leading jets."""
    jets = events[object_name] if object_name is not None else events.Jet
    jets = ak.pad_none(jets, 2) # Ensure we have at least 2 jets, pad with None if not
    jet1 = jets[:,0]
    jet2 = jets[:,1]
    delta_etajj = abs(jet1.eta - jet2.eta)
    delta_etajj = ak.fill_none(delta_etajj, -999)
    return delta_etajj

def compute_delta_rjj(events, object_name=None):
    """Compute the delta R between the two leading jets."""
    jets = events[object_name] if object_name is not None else events.Jet
    jets = ak.pad_none(jets, 2) # Ensure we have at least 2 jets, pad with None if not
    delta_r = jets[:,0].delta_r(jets[:,1])
    delta_r = ak.fill_none(delta_r, -999)
    return delta_r

def compute_delta_phijj(events, object_name=None):
    """Compute the delta phi between the two leading jets."""
    jets = events[object_name] if object_name is not None else events.Jet
    jets = ak.pad_none(jets, 2) # Ensure we have at least 2 jets, pad with None if not
    jet1 = jets[:,0]
    jet2 = jets[:,1]
    delta_phi = abs(jet1.phi - jet2.phi)
    delta_phi = ak.fill_none(delta_phi, -999)
    return delta_phi

def compute_mt(events, object_name=None):
    """
    Compute the transverse mass of the leading lepton and MET.
    object_name should be a list of two strings: [lepton_collection, met_collection]
    If not provided, defaults to "Lepton" and "MET".
    """
    if object_name is not None:
        lepton_field = object_name[0]
        met_field = object_name[1]
    leptons = ak.pad_none(events[lepton_field], 1)
    lepton = leptons[:,0] # Leading lepton
    met = events[met_field]
    mt = np.sqrt(2 * lepton.pt * met.pt * (1 - np.cos(lepton.phi - met.phi)))
    mt = ak.fill_none(mt, -999)
    return mt