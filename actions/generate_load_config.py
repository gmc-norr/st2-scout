from pathlib import Path
import os
import re
from datetime import datetime
from typing import Optional

from st2common.runners.base_action import Action

import logging

log = logging.getLogger(__name__)

PIPELINE_RD = "nf-core/raredisease"
PIPELINE_CANCER = "genomic-medicine-sweden/Twist_Solid"

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
    ):

        try:

            case_entry = self._case_entry(
                case_files=case_files,
                case_id=case_id,
                case_name=case_name.replace("/", "_").replace("-", "_").replace(",", "_").replace(".", "_"),
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
        pat = re.compile(r"^(.+)_(PAN|SP)_WGS_v\.?\d+\.\d+$")
        m = pat.match(igene_panel)
        if m is None:
            raise ValueError(f"unknown panel: {igene_panel}")
        scout_panel = m.group(1)
        # Super-panels should include the suffix
        if m.group(2) == "SP":
            scout_panel += "_SP"
        # Stupid special case
        if scout_panel == "HTAD":
            return scout_panel.lower()
        return scout_panel

    def _case_entry(
        self,
        case_files: list,
        case_id: str,
        case_name: str,
        pipeline: str,
        panels: list,
    ) -> dict:

        owner = self.config["owners_map"][pipeline]  # get from pack configuration
        genome = self.config["genomes_map"][pipeline]
        case_entry = {}

        if pipeline == PIPELINE_RD:
            scout_files = self._parse_files(case_files, level="case")
            case_entry = {
                "family": case_id,
                "family_name": case_name if case_name is not None else case_id,
                "human_genome_build": genome,
                "rank_model_version": "0.1",
                "owner": owner,
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
                case_entry["default_panels"] = default_panels

        # TODO fix case_entry for gms-solid

        return case_entry

    def _sample_entries(self, sample_ids, sample_files: dict, sample_info: dict) -> list:

        sample_entries = []
        for sample_id in sample_ids:
            parsed_files = self._parse_files(sample_files[sample_id], level="sample")
            sample_entry = {}
            for scout_name, file_path in parsed_files.items():
                if scout_name in self.config["scout_chromograph_file_prefixes"]:
                    if sample_entry.get("chromograph_images") is None:
                        sample_entry["chromograph_images"] = {}
                    sample_entry["chromograph_images"][scout_name] = file_path
                else:
                    sample_entry[scout_name] = file_path
            sample_entry["sex"] = self._parse_sex(sample_info[sample_id].get("Sex"))
            sample_entry["sample_id"] = sample_id
            sample_entry["sample_name"] = sample_id
            sample_entry["phenotype"] = "affected" #TODO change when running trios
            sample_entry["analysis_type"] = "wgs" # TODO change when running gms-solid
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
                for scout_key, file_suffix in self.config["scout_case_file_suffixes"].items():
                    if file_path.endswith(file_suffix):
                        parsed_files[scout_key] = file_path
                        break

        if level == "sample":
            for file in files:
                is_prefix = False
                file_path = file["path"]
                for _, file_prefix in self.config["scout_chromograph_file_prefixes"].items():
                    if file_prefix in file_path:
                        is_prefix = True

                if not is_prefix:
                    if not Path(file_path).exists():
                        raise FileNotFoundError(f"{file_path} does not exist")
                    for scout_key, file_suffix in self.config["scout_sample_file_suffixes"].items():
                        if file_path.endswith(file_suffix):
                            parsed_files[scout_key] = file_path
                            break

                else:
                    for (
                        scout_key,
                        file_prefix,
                    ) in self.config["scout_chromograph_file_prefixes"].items():
                        if file_prefix in file_path:
                            parsed_files[scout_key] = (
                                file_path.split(file_prefix)[0] + file_prefix
                            )

        return parsed_files
