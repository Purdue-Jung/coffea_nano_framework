"""
This module defines analysis variables
"""
import awkward as ak

def get_variable(events, variable_name):
    """Get the variable from the events metadata."""
    available_variables = {
        "mjj": compute_mjj, # type: ignore
        "deltaEtajj": compute_delta_etajj, # type: ignore
        "deltaRjj": compute_delta_rjj # type: ignore
    }
    if variable_name not in available_variables:
        raise ValueError(f"Variable {variable_name} is not defined.")
    return available_variables[variable_name](events)

def compute_mjj(events):
    """Compute the invariant mass of the two leading jets."""
    jets = ak.pad_none(events.Jet, 2) # Ensure we have at least 2 jets, pad with None if not
    jet1 = jets[:,0]
    jet2 = jets[:,1]
    mjj = (jet1 + jet2).mass
    return mjj

def compute_delta_etajj(events):
    """Compute the delta eta between the two leading jets."""
    jets = ak.pad_none(events.Jet, 2) # Ensure we have at least 2 jets, pad with None if not
    jet1 = jets[:,0]
    jet2 = jets[:,1]
    delta_etajj = abs(jet1.eta - jet2.eta)
    return delta_etajj

def compute_delta_rjj(events):
    """Compute the delta R between the two leading jets."""
    jets = ak.pad_none(events.Jet, 2) # Ensure we have at least 2 jets, pad with None if not
    delta_r = jets[:,0].delta_r(jets[:,1])
    return delta_r
