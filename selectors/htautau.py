"""
    Processor for dilepton selection
"""
import numpy as np
import awkward as ak
from selection.processor import SelectionProcessor
from selection.selection_utils import lepton_merging, dilepton_pairing, get_4vector_sum,\
    delta_r, add_to_obj, update_collection
import corrections.JME as JME
import corrections.LUM as LUM
import corrections.EGM as EGM
import corrections.MUO as MUO
import corrections.TAU as TAU

class Selector(SelectionProcessor):
    """Processor for dilepton ttbar event selection and tree creation."""
    def __init__(self, selection_cfg):
        super().__init__(selection_cfg)
        self.step_tag = "tree_variables_"
        # Additional initialization can be added here

    def pre_selection(self, events):
        """Pre-selection steps before main selection process."""
        super().pre_selection(events)
        # Any pre-selection steps can be added here
        events = LUM.pileup_weights(events, self.cfg)

        ### Object selection
        ## Correction
        events = EGM.electron_corr(events, self.cfg)
        ## Electron selection
        electron = events.Electron
        n_electrons = ak.sum(ak.num(electron))
        # Pt cut
        electron = electron[electron.corr_pt >= 10.0]
        print(f"Electron pt selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # Eta cut
        electron = electron[np.abs(electron.eta) <= 2.5]
        print(f"Electron eta selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # Eta clustering cut
        electron = electron[
            (np.abs(electron.eta) > 1.566) | (np.abs(electron.eta) < 1.4442)
            ]
        print(f"Electron eta clustering selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # ID cut
        # electron = electron[electron.cutBased >= 4] # Tight
        # print(f"Electron ID selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # dxy cut
        electron = electron[np.abs(electron.dxy) <= 0.045]
        print(f"Electron dxy selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # dz cut
        electron = electron[np.abs(electron.dz) <= 0.2]
        print(f"Electron dz selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")
        # Iso cut
        # electron = electron[electron.miniPFRelIso_all <= 0.5]
        electron = electron[electron.mvaNoIso_WP90] # boolean mask
        electron = EGM.electron_sf(electron, "wp90noiso", self.cfg)
        print(f"Electron mvaNoIso_WP90 selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")

        # Conversion Veto
        electron = electron[electron.convVeto]
        print(f"Electron conversion veto selection efficiency: {ak.sum(ak.num(electron))*100/n_electrons:.2f}%")

        n_selected_electrons = ak.sum(ak.num(electron))
        print(f"Electron total selection efficiency: {n_selected_electrons*100/n_electrons:.2f}%")

        events = update_collection(events, "Electron", electron)

        ## Muon selection
        ## correction
        events = MUO.muon_corr(events, self.cfg)
        muon = events.Muon
        n_muons = ak.sum(ak.num(muon))
        # Pt cut
        muon = muon[muon.corr_pt >= 10.0]
        print(f"Muon pt selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        # Eta cut
        muon = muon[np.abs(muon.eta) <= 2.4]
        print(f"Muon eta selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        # Iso cut
        muon = muon[muon.pfRelIso04_all <= 0.5]
        print(f"Muon isolation selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        # ID cut
        muon = muon[muon.mediumId] # boolean mask
        muon = MUO.muon_sf(muon, "NUM_MediumID_DEN_TrackerMuons", self.cfg)
        muon = MUO.muon_sf(muon, "NUM_TightPFIso_DEN_MediumID", self.cfg)
        print(f"Muon ID selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        # dxy cut
        muon = muon[np.abs(muon.dxy) <= 0.045]
        print(f"Muon dxy selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        # dz cut
        muon = muon[np.abs(muon.dz) <= 0.2]
        print(f"Muon dz selection efficiency: {ak.sum(ak.num(muon))*100/n_muons:.2f}%")
        n_selected_muons = ak.sum(ak.num(muon))
        print(f"Muon total selection efficiency: {n_selected_muons*100/n_muons:.2f}%")

        events = update_collection(events, "Muon", muon)

        ## Tau selection
        ## correction
        events = TAU.tau_sf_corr(events,
                            working_points={
                                "e_to_tau": "VVLoose",
                                "mu_to_tau": "VLoose",
                                "jet_to_tau": "Medium"
                            },
                            cfg=self.cfg,
                            dependency="pt"
                            )
        
        tau = events.Tau
        n_taus = (ak.sum(ak.num(tau)))
        tauprod = events.TauProd

        tau_criteria = (tau.pt >= 20.0)
        print(f"Tau pt >= 20 selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")
        
        tau_criteria = tau_criteria & (tau.eta <= 2.5)
        print(f"Tau eta <= 2.5 selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")

        tau_criteria = tau_criteria & (tau.idDeepTau2018v2p5VSe >= 2)
        print(f"Tau ID tau vs electron selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")

        tau_criteria = tau_criteria & (tau.idDeepTau2018v2p5VSmu >= 1)
        print(f"Tau ID tau vs muon selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")

        tau_criteria = tau_criteria & (tau.idDeepTau2018v2p5VSjet >= 5)
        print(f"Tau ID tau vs jet selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")

        tau_criteria = tau_criteria & (np.abs(tau.dz) <= 0.2)
        print(f"Tau dz selection efficiency: {ak.sum(tau_criteria)*100/n_taus:.2f}%")

        selected_taus = tau[tau_criteria]
        n_selected_taus = ak.sum(ak.num(selected_taus))
        print(f"Tau selection efficiency: {n_selected_taus*100/n_taus:.2f}%")

        # Modify TauProd_tauIdx
        max_ntau = ak.max(ak.num(tau))
        rect_tau_criteria = ak.fill_none(ak.pad_none(tau_criteria, max_ntau, axis=1), False)
        rect_new_tauidx = ak.from_numpy(np.cumsum(ak.to_numpy(rect_tau_criteria), axis=1) - 1)
        # rect_new_tauidx[~rect_tau_criteria] = -1
        rect_new_tauidx = ak.where(rect_tau_criteria, rect_new_tauidx, -1)
        new_tauprod_tauidx = rect_new_tauidx[tauprod.tauIdx]

        tauprod_mask = new_tauprod_tauidx >= 0
        tauprod = tauprod[tauprod_mask]
        new_tauprod_tauidx = new_tauprod_tauidx[tauprod_mask]
        
        assert not ak.any(new_tauprod_tauidx < 0)
        events = update_collection(events, "Tau", selected_taus)
        tauprod = update_collection(tauprod, "tauIdx", new_tauprod_tauidx)
        events = update_collection(events, "TauProd", tauprod)

        ## Merge electrons and muons into leptons
        events["lepton"] = lepton_merging(events, include_tau=True)
        events["lep"], events["lbar"] = dilepton_pairing(events.lepton)
        events["llbar"] = get_4vector_sum(events.lep, events.lbar, corrected=True)

        ## Define reco channels
        pdg_lep = events.lep.pdgId
        pdg_lbar = events.lbar.pdgId
        self.channels = {
            "etau": (pdg_lep == 11) & (pdg_lbar == -15) | (pdg_lep == 15) & (pdg_lbar == -11),
            "mutau": (pdg_lep == 13) & (pdg_lbar == -15) | (pdg_lep == 15) & (pdg_lbar == -13),
            "tautau": (pdg_lep == 15) & (pdg_lbar == -15)
        }

        ## Add to jetsAK4
        events = add_to_obj(
            events, "Jet",
            {
                "DeltaR_lep": delta_r(events.Jet, events.lep),
                "DeltaR_lbar": delta_r(events.Jet, events.lbar)
            }
        )

        ## jetsAK4 selection
        jets = events.Jet
        n_jets = (ak.sum(ak.num(jets)))
        # Remove Lepton Overlap
        jets_idx = ak.local_index(jets.pt)
        lep_mask = ~(jets_idx == events.lep.jetIdx)
        lbar_mask = ~(jets_idx == events.lbar.jetIdx)
        jets = jets[lep_mask & lbar_mask]
        # ID selection
        if "jetId" in jets.fields:
            # Following JME recommendations for jet ID
            # https://twiki.cern.ch/twiki/bin/view/CMS/JetID13p6TeV
            mask1 = (np.abs(jets.eta) <= 2.7) & (jets.jetId >= 2)
            mask2 = ((np.abs(jets.eta) > 2.7) & (np.abs(jets.eta) <= 3.0)) & \
                (jets.jetId >= 2) & (jets.neHEF < 0.99)
            mask3 = (np.abs(jets.eta) > 3.0) & (jets.jetId >= 2) & (jets.neEmEF < 0.4)
            #mask_tight = mask1 | mask2 | mask3
            mask4 = (np.abs(jets.eta) <= 2.7) & mask1 & (jets.muEF < 0.8) & (jets.chEmEF < 0.8)
            mask_lepveto = mask4 | mask2 | mask3
            jets = jets[mask_lepveto]
        else:
            jets = JME.jet_id(jets, "AK4PUPPI_TightLeptonVeto", self.cfg)
        # jet energy correction
        jets = JME.jet_jerc(events, jets, self.cfg)
        # Pt cut
        jets = jets[jets.corr_pt > 30.0]
        # Eta cut
        jets = jets[np.abs(jets.eta) < 2.5]
        # # cleaning cut
        # jets = jets[
        #     (jets.DeltaR_lep > 0.4) & (jets.DeltaR_lbar > 0.4)
        # ]
        # veto map
        jets = jets[JME.veto_map(jets,"jetvetomap",self.cfg)]

        n_selected_jets = ak.sum(ak.num(jets))
        print(f"Jet selection efficiency: {n_selected_jets*100/n_jets:.2f}%")

        events = update_collection(events, "Jet_selected", jets)

        # tagger = "UParTAK4" if self.cfg["era"] in ["2024", "2025"]\
        #     else "robustParticleTransformer"
        # corr_type = "kinfit" if self.cfg["era"] in ["2024", "2025"] else "shape"
        # ## B-Jet selection
        # print(f"Applying BTV corrections with tagger {tagger} and correction type {corr_type}")
        # events, bjets = BTV.btagging(events, "Jet_selected", tagger,
        #                                     "M", self.cfg, correction_type=corr_type)
        # events["bJetsAK4"] = bjets

        ## Gen Information (Must Compute It)
        # if self.cfg['isSignal'] == "True":
        #     events["genTTbar"] = get_4vector_sum(events.genTop, events.genTBar)
        #     events["genLLbar"] = get_4vector_sum(events.genLepton, events.genLepBar)

        #     # gen-level dilepton channels
        #     gen_pdg_lep = events.genLepton.pdgId
        #     gen_pdg_lbar = events.genLepBar.pdgId
        #     self.gen_channels = {
        #         "ee": (gen_pdg_lep == 11) & (gen_pdg_lbar == -11),
        #         "mumu": (gen_pdg_lep == 13) & (gen_pdg_lbar == -13),
        #         "emu": ((gen_pdg_lep == 13) & (gen_pdg_lbar == -11))\
        #             | ((gen_pdg_lep == 11) & (gen_pdg_lbar == -13))
        #     }
        return events

    def event_selection(self, events):
        """Dilepton selection process."""
        super().event_selection(events)

        # self.step0_snapshot(events)

        ### Main event selection
        # initialize selector
        self.init_selection()

        # Based on twiki recommendations
        # https://twiki.cern.ch/twiki/bin/viewauth/CMS/MissingETOptionalFiltersRun2
        flags = [
            'goodVertices',
            'globalSuperTightHalo2016Filter',
            'EcalDeadCellTriggerPrimitiveFilter',
            'BadPFMuonFilter',
            'BadPFMuonDzFilter',
            'hfNoisyHitsFilter',
            'eeBadScFilter'
        ]
        if self.cfg['era'] == '2024':
            flags.append('ecalBadCalibFilter')

        met_filters = events.event > 0 # initialize to true mask
        for flag in flags:
            met_filters = met_filters & events.Flag[flag]

        # step1a
        self.add_selection_step(
            step_label="METFilters",
            mask=met_filters,
            parent="init"
        )

        self.add_selection_step(
            step_label="Triggers",
            mask={
                chan: self.dilepton_hlt_mask(
                    events, chan, self.cfg
                )
                for chan in self.channels
            },
            channel_wise=True,
            parent = "METFilters"
        )

        # step1
        self.add_selection_step(
            step_label="PrimaryVertex",
            mask=(events.PV.npvsGood > 0),
            parent="Triggers"
        )

        # step3
        self.add_selection_step(
            step_label="LeptonInvariantMass",
            mask=(events.llbar.mass > 20),
            parent="PrimaryVertex"
        )

        # step4
        self.add_selection_step(
            step_label="JetMultiplicity",
            mask=(ak.num(events.Jet_selected, axis=1) >= 2),
            parent="LeptonInvariantMass"
        )

        # self.create_cutflow_histograms(events, step7)

        self.make_snapshot(events, "METFilters", step_name="stepMET")
        self.make_snapshot(events, "Triggers", step_name="stepHLT")
        self.make_snapshot(events, "PrimaryVertex", step_name="stepPV")
        self.make_snapshot(events, "LeptonInvariantMass", step_name="stepLepInvMass")
        self.make_snapshot(events, "JetMultiplicity", step_name="stepJetMult", save_cutflow=True)

        return events

    def dilepton_hlt_mask(self, events, channel, cfg):
        """
        Create HLT mask for dilepton channels
        Args:
            events: Awkward Array with event information
            channel: str, dilepton channel ("ee", "mumu", "emu")
            hlt_map: dict, mapping of HLT paths to indices
                (Not really used in current implementation)
            cfg: dict, configuration dictionary
        Returns:
            Awkward Array boolean mask for events passing HLT for the given channel
        """

        def false_mask():
            return ak.full_like(events.event, False, dtype=bool)

        # Build masks for all groups present in cfg["HLT"]
        # which would be defined in config/selection/HLT.yml
        tot_masks = {}
        for grp, grp_hlt in cfg["HLT"].items():
            # For data, if dataset is incompatible with this group, use false mask
            if cfg["isData"] == "True":
                dataset = cfg["process"].split("_")[-1]
                dataset = dataset[0].upper() + dataset[1:] if dataset else ""
                if dataset not in grp_hlt["datasets"]:
                    print(f"Dataset {dataset} not in datasets for group {grp}. Using false mask.")
                    tot_masks[grp] = false_mask()
                    continue
            mask = ak.zeros_like(events.event, dtype=bool)
            for path in grp_hlt["triggers"]:
                if path in events.HLT.fields:
                    mask = mask | events.HLT[path]
                else:
                    print(f"WARNING: HLT path {path} not found in events.HLT fields.")
            tot_masks[grp] = mask

        # Helper to get a mask safely
        def M(name):
            return tot_masks.get(name, false_mask())

        ## Be careful with the following logic where data and MC differ in HLT combination
        # Combine per data/MC and channel
        if cfg["isData"] == "False":
            # MC: use union of the groups relevant for this dilepton channel
            match channel:
                case "ee":
                    tot_mask = M("ee") | M("se")
                case "mumu":
                    tot_mask = M("mumu") | M("smu")
                case "emu":
                    tot_mask = M("emu") | M("se") | M("smu")
                case "tautau":
                    tot_mask = M("tautau")
                case "etau":
                    tot_mask = M("etau") | M("se") # placeholder, will be defined in data section
                case "mutau":
                    tot_mask = M("mutau") | M("smu") # placeholder, will be defined in data section
                case _:
                    raise ValueError(f"Channel {channel} not supported.")
            return tot_mask
        else:
            # Data: dataset-specific logic with anti-overlaps
            if channel == "ee":
                match dataset:
                    case "EGamma":
                        tot_mask = M("ee") | M("se")
                    case _:
                        print(f"Dataset {dataset} not supported for channel {channel} in data."
                            " Returning false mask.")
                        # use events.event to create a false mask
                        tot_mask = false_mask()
            elif channel == "emu":
                match dataset:
                    case "MuonEG":
                        tot_mask = M("emu")
                    case "EGamma":
                        tot_mask = M("se") & ~M("emu")
                    case "SingleMuon":
                        tot_mask = M("smu") & ~M("emu") & ~M("se")
                    case "Muon":
                        tot_mask = M("smu") & ~M("emu") & ~M("se")
                    case _:
                        print(f"Dataset {dataset} not supported for channel {channel} in data. "
                            "Returning false mask.")
                        tot_mask = false_mask()
            elif channel == "mumu":
                match dataset:
                    case "Muon":
                        tot_mask = M("mumu") | M("smu")
                    case "SingleMuon":
                        tot_mask = ~M("mumu") & M("smu")
                    case "DoubleMuon":
                        tot_mask = M("mumu")
                    case _:
                        print(f"Dataset {dataset} not supported for channel {channel} in data. "
                            "Returning false mask.")
                        tot_mask = false_mask()
            else:
                raise ValueError(f"Channel {channel} not supported.")
            return tot_mask
