from pathlib import Path
import os
import re
from datetime import datetime
from typing import Optional

import logging

log = logging.getLogger(__name__)


try:
    from st2common.runners.base_action import Action
except ImportError:

    class Action:
        def __init__(self, config=None):
            self.config = config


PIPELINE_CASE_FILES = {
    "clinical_snv": "rank_and_filter/{case}_snv_ranked_clinical.vcf.gz",
    "research_snv": "rank_and_filter/{case}_snv_ranked_research.vcf.gz",
    "peddy_ped": "peddy/{case}.peddy.ped",
    "peddy_sex": "peddy/{case}.sex_check.csv",
    "peddy_check": "peddy/{case}.ped_check.csv",
    "multiqc": "multiqc/multiqc_report.html",
    "smn_tsv": "smncopynumbercaller/out/{case}_smncopynumbercaller.tsv",
}


PIPELINE_SAMPLE_FILES = {
    "d4": {
        "path": "qc_bam/{sample}_mosdepth.per-base.d4",
        "is_prefix": False,
    },
    "bam": {
        "path": "alignment/{sample}_sorted_md.bam",
        "is_prefix": False,
    },
    "chromograph_autozygous": {
        "path": "annotate_snv/genome/{sample}_rhocallviz_autozyg_chromograph/{sample}_rhocallviz_chr",
        "is_prefix": True,
    },
    "chromograph_coverage": {
        "path": "qc_bam/{sample}_chromographcov/{sample}_tidditcov_chr",
        "is_prefix": True,
    },
}


PIPELINE_RD = "raredisease"
PIPELINE_CANCER = "gms-solid"

OWNERS = {
    PIPELINE_RD: "clingen-rd",
    PIPELINE_CANCER: "clingen-cancer",
}

GENOME = {
    PIPELINE_RD: "38",
    PIPELINE_CANCER: "37",
}


class GenerateLoadConfigAction(Action):
    """Action for creating load configs for scout"""

    def run(
        self,
        sample_ids: list,
        case_id: str,
        case_name: str,
        sample_files: dict,
        case_files: list,
        pipeline: str,
        igene_panels: list,
    ):

        try:
            case_entry = self._case_entry(
                case_files=case_files,
                case_id=case_id,
                case_name=case_name,
                pipeline=pipeline,
                panels=igene_panels,
            )
            sample_entries = self._sample_entries(
                sample_ids=sample_ids, sample_files=sample_files
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

        return case_entry

    def _sample_entries(self, sample_ids, sample_files: dict) -> list:

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
            sample_entries.append(sample_entry)
        return sample_entries

    def _parse_files(self, files: list, level: str):

        parsed_files = {}

        if level not in ("case", "sample"):
            log.warning("level must be 'case' or 'sample'")
        if level == "case":
            for file in files:
                file_path = file["path"]
                if not Path(file_path).exists():
                    raise FileNotFoundError(f"{file_path} does not exist")
                for scout_key, file_suffix in self.config["scout_case_file_suffxies"].items():
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
                    ) in SCOUT_CHROMOGRAPH_FILE_PREFIXES.items():
                        if file_prefix in file_path:
                            parsed_files[scout_key] = (
                                file_path.split(file_prefix)[0] + file_prefix
                            )

        return parsed_files


SCOUT_CASE_FILE_SUFFIXES = {
    "vcf_snv": "_snv_ranked_clinical.vcf.gz",
    "vcf_snv_research": "_snv_ranked_research.vcf.gz",
    "peddy_ped": ".peddy.ped",
    "peddy_sex": ".sex_check.csv",
    "peddy_check": ".ped_check.csv",
    "multiqc": "multiqc_report.html",
    "smn_tsv": "_smncopynumbercaller.tsv",
}

SCOUT_SAMPLE_FILE_SUFFIXES = {
    "d4_path": "_mosdepth.per-base.d4",
    "alignment_path": "_sorted_md.bam",
}

SCOUT_CHROMOGRAPH_FILE_PREFIXES = {
    "autozygous": "_rhocallviz_chr",
    "coverage": "_tidditcov_chr",
}
