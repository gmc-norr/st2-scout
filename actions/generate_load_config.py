from pathlib import Path
import os
import re
from datetime import datetime
from typing import Optional

from st2common.runners.base_action import Action

import logging

log = logging.getLogger(__name__)

class GenerateLoadConfigAction(Action):
    """Action for creating load configs for scout"""

    def run(
        self,
        sample_ids: list,
        case_id: str,
        case_name: str,
        sample_files: dict,
        sample_info: dict,
        case_files: list,
        pipeline: str,
        igene_panels: list,
        scout_specifics: dict,
    ):

        try:
            self.pipeline_specs = scout_specifics

            case_entry = self._case_entry(
                case_files=case_files,
                case_id=case_id,
                case_name=case_name.replace("/", "-").replace(",", "-").replace(".", "-"),
                pipeline=pipeline,
                panels=igene_panels,
            )
            sample_entries = self._sample_entries(
                sample_ids=sample_ids, sample_files=sample_files, sample_info=sample_info
            )
            case_entry["samples"] = sample_entries
            return (True, case_entry)
        except Exception as e:
            return (False, {"error": str(e)})

    def _get_scout_panels(self, panels: list) -> tuple:
        default_panels = []
        for p in panels:
            if p == "SNV_WGS":
                continue
            default_panels.append(self._scout_panel_from_igene_panel(p))
            log.info(f"using scout panel {default_panels[-1]} (iGene panel {p})")

        default_panels = list(set(default_panels))
        all_panels = default_panels + ["PANELAPP-GREEN"]

        return (default_panels, all_panels)

    def _scout_panel_from_igene_panel(self, igene_panel: str) -> str:

        pat = re.compile(r"^(.+)_(PAN|SP)_(WGS|GMS560)_v\.?\d+\.\d+$")
        m = pat.match(igene_panel)
        if m is None:
            raise ValueError(f"unknown panel: {igene_panel}")
        scout_panel = m.group(1)

        if m.group(3) == "GMS560":
            return scout_panel.lower()

        elif m.group(3) == "WGS":
            # Super-panels should include the suffix
            if m.group(2) == "SP":
                scout_panel += "_SP"
            # Stupid special case
            if scout_panel == "HTAD":
                return scout_panel.lower()
            return scout_panel

        else:
            raise ValueError(f"Unknown panel {m.group(0)}")

    def _case_entry(
        self,
        case_files: list,
        case_id: str,
        case_name: str,
        pipeline: str,
        panels: list,
    ) -> dict:

        owner = self.pipeline_specs["owner"]
        genome = self.pipeline_specs["genome"]
        rank_model_url = self.pipeline_specs["rankmodel"]
        track = self.pipeline_specs["track"]
        case_entry = {}

        scout_files = self._parse_files(case_files, level="case")
        case_entry = {
            "family": case_id,
            "family_name": case_name if case_name is not None else case_id,
            "human_genome_build": genome,
            "rank_model_version": "0.1",
            "owner": owner,
            "rank_model_url": rank_model_url,
            "track": track
        }
        # Add case specific files
        for scout_file, file in scout_files.items():
            case_entry[scout_file] = file
        # Get analysis date from multiqc
        multiqc = scout_files.get("multiqc")
        if multiqc is None:
            analysis_date = datetime.now()
        else:
            info = os.stat(multiqc)
            analysis_date = datetime.fromtimestamp(info.st_mtime)
        case_entry["analysis_date"] = analysis_date
        default_panels, all_panels = self._get_scout_panels(panels)

        if len(all_panels) > 0:
            case_entry["gene_panels"] = all_panels
        if len(default_panels) > 0:
            case_entry["default_gene_panels"] = default_panels

        if self.pipeline_specs.get("biomarker_file_suffixes") is not None:
            biomarkers = self._get_biomarkers(case_files)
            for key, value in biomarkers.items():
                case_entry[key] = value

        return case_entry

    def _sample_entries(self, sample_ids, sample_files: dict, sample_info: dict) -> list:

        sample_entries = []
        for sample_id in sample_ids:
            parsed_files = self._parse_files(sample_files[sample_id], level="sample")
            sample_entry = {}
            chromograph_prefixes = self.pipeline_specs.get("scout_chromograph_file_prefixes", {})
            for scout_name, file_path in parsed_files.items():
                if scout_name in chromograph_prefixes:
                    if sample_entry.get("chromograph_images") is None:
                        sample_entry["chromograph_images"] = {}
                    sample_entry["chromograph_images"][scout_name] = file_path
                else:
                    sample_entry[scout_name] = file_path
            sample_entry["sex"] = self._parse_sex(sample_info[sample_id].get("Sex"))
            sample_entry["sample_id"] = sample_id
            sample_entry["sample_name"] = sample_id
            sample_entry["phenotype"] = "affected" #TODO change when running trios
            sample_entry["analysis_type"] = self.pipeline_specs["analysis_type"]
            sample_entries.append(sample_entry)
        return sample_entries

    def _parse_sex(self, igene_sex):
        if igene_sex == "F":
            return "female"
        if igene_sex == "M":
            return "male"
        return igene_sex


    def _parse_files(self, files: list, level: str):

        parsed_files = {}

        if level not in ("case", "sample"):
            log.warning("level must be 'case' or 'sample'")
        if level == "case":
            for file in files:
                file_path = file["path"]
                if not Path(file_path).exists():
                    raise FileNotFoundError(f"{file_path} does not exist")
                for scout_key, file_suffix in self.pipeline_specs["scout_case_file_suffixes"].items():
                    if file_path.endswith(file_suffix):
                        parsed_files[scout_key] = file_path
                        break

        if level == "sample":
            for file in files:
                is_prefix = False
                file_path = file["path"]

                if "scout_chromograph_file_prefixes" in self.pipeline_specs:
                    for _, file_prefix in self.pipeline_specs["scout_chromograph_file_prefixes"].items():
                        if file_prefix in file_path:
                            is_prefix = True

                if not is_prefix:
                    if not Path(file_path).exists():
                        raise FileNotFoundError(f"{file_path} does not exist")
                    for scout_key, file_suffix in self.pipeline_specs["scout_sample_file_suffixes"].items():
                        if file_path.endswith(file_suffix):
                            parsed_files[scout_key] = file_path
                            break

                else:
                    for (
                        scout_key,
                        file_prefix,
                    ) in self.pipeline_specs["scout_chromograph_file_prefixes"].items():
                        if file_prefix in file_path:
                            parsed_files[scout_key] = (
                                file_path.split(file_prefix)[0] + file_prefix
                            )

        return parsed_files

        def _get_biomarkers(self, case_files: list):
            biomarker_files = dict()
            if self.pipeline_specs.get("biomarker_file_suffixes") is None:
                return None
            for file in case_files:
                for key, value in self.pipeline_specs["biomarker_file_suffixes"].items():
                    if file.endswith("value"):
                        if not Path(file).exists():
                            raise FileNotFoundError(f"{file} does not exist")
                        biomarker_files[key] = Path(file)

            biomarkers = dict()
            if "tmb" in biomarker_files:
                biomarkers["tmb"] = self._parse_tmb(biomarker_files["tmb"])

            if "hrd" in biomarker_files:
                biomarkers["hrd"] = self._parse_hrd(biomarker_files["hrd"])

            if "msi" in biomarker_files:
                biomarkers["msi"] = self._parse_msi(biomarker_files["msi"])

            return biomarkers

        def _get_hrd(self, hrd_file: Path):
            """
            Reads HRD file with two lines
            - header line ('HRD-score HRD Telomeric_AI LST')
            - values
            Returns the first column (HRD-score) as an int.
            """
            with open(hrd_file, "r") as f:
                header = f.readline().strip().split()
                values = f.readline().strip().split()
            hrd_data = dict(zip(header, values))
            return str(int(hrd_data["HRD-score"]))

        def _parse_tmb(self, tmb_file: Path):
            """
            Reads TMB file from gms-solid where first line looks like
            'TMB:   <tmb-value>'
            and returns the numeric TMB as a float
            """
            with open(tmb_file, "r") as f:
                tmb_line = f.readline().strip()
            tmb_value = tmb_line.split(':')[1].strip()
            return str(float(tmb_value))

        def _get_msi(self, msi_file: Path):
            """
            Parse a MSI file with header and a single line of values
            - header line: 'Total_Number_of_Sites   Number_of_Somatic_Sites %'
            - values
            Returns the MSI percentage under '%' as a float
            """
            with open(msi_file, "r") as f:

                header = f.readline().strip().split()
                values = f.readline().strip().split()

            data = dict(zip(header, values))
            return str(float(data["%"]))