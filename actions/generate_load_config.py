from pathlib import Path
import yaml
import os
import re
import csv
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



PIPELINE_OUTPUT_DIR = "raredisease_results"

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
PIPLELINE_CANCER = "gms-solid"

OWNERS = {
    PIPELINE_RD: "clingen-rd",
    PIPLELINE_CANCER: "clingen-cancer",
    }

GENOME = {
    PIPELINE_RD: "38",
    PIPLELINE_CANCER: "37",
    }

class PipelineNameException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ParentIDsException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class GenerateLoadConfigAction(Action):
    """ Action for creating load configs for scout """
    def run(self, samples: list, pipeline_dir: str, pipeline: str, panels: list = [], default_panels: list = []):
        self.pipeline = pipeline
        self.pipeline_dir = Path(pipeline_dir)
        try:
            case_id = self._get_case_id(self.pipeline_dir)
            case_name = None
            parent_ids = [sample.get("ParentSampleID", "").replace("/", "-").replace(" ", "_") for sample in samples]

            
            owner = OWNERS[self.pipeline]
            genome = GENOME[self.pipeline]


            if len(set(parent_ids)) != 1:
                raise ParentIDsException("More than one parent ID for samples")
            
            if parent_ids[0] != "":
                case_name = parent_ids[0]
        
            scout_panels = []
            for sample in samples:
                for p in sample.get("Panels", []):
                    if p == "SNV_WGS":
                        continue
                    scout_panels.append(self._scout_panel_from_igene_panel(p))
                    log.info(f"using scout panel {scout_panels[-1]} (iGene panel {p})")

            all_default_panels = list(set(default_panels + scout_panels))
            all_panels = list(set(panels + all_default_panels + ["PANELAPP-GREEN"]))

            if self.pipeline not in (PIPELINE_RD, PIPLELINE_CANCER):
                raise PipelineNameException(f"{self.pipeline} is not a defined pipeline") 

            if self.pipeline == PIPELINE_RD:
                analysis_date = self._get_analysis_date(Path(self.pipeline_dir))
                config = {
                    "family": case_id,
                    "family_name": case_name if case_name is not None else case_id,
                    "analysis_date": analysis_date,
                    "human_genome_build": genome,
                    "rank_model_version": "0.1",
                    "owner": owner,
                    "samples": self._build_sample_entries(samples),
                    "vcf_snv": self._case_file(self.pipeline_dir, "clinical_snv", case_id),
                    "vcf_ranked": self._case_file(self.pipeline_dir, "research_snv", case_id),
                    "peddy_ped": self._case_file(self.pipeline_dir, "peddy_ped", case_id),
                    "peddy_sex": self._case_file(self.pipeline_dir, "peddy_sex", case_id),
                    "peddy_check": self._case_file(self.pipeline_dir, "peddy_check", case_id),
                    "multiqc": self._case_file(self.pipeline_dir, "multiqc", case_id),
                    "smn_tsv": self._case_file(self.pipeline_dir, "smn_tsv", case_id),
                }
            
                if len(all_panels) > 0:
                    config["gene_panels"] = all_panels
                if len(all_default_panels) > 0:
                    config["default_panels"] = all_default_panels

            elif self.pipeline == PIPLELINE_CANCER:
                #TODO WHEN GMS560 IS IN PRODUCTION
                config = {}

            output_path = self._write_yaml(config, self.pipeline_dir)
            return (True, {
                "generated": True,
                "case_id": case_id,
                "config_path": output_path,
                "panels": all_panels,
            })

        except Exception as e:
            return (False, {
                "generated": False,
                "error": str(e),
            })
        
    def _scout_panel_from_igene_panel(self, igene_panel):
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

    def _read_samplesheet(self, dir: Path) -> csv.DictReader:
        samplesheet = dir / "samplesheet.csv"
        if not samplesheet.exists():
            raise IOError("could not find samplesheet")
        with open(samplesheet) as f:
            r = csv.DictReader(f.readlines())
        return r

    def _get_case_id(self, dir: Path) -> str:
        case_id = None
        try:
            r = self._read_samplesheet(dir)
        except:
            raise
        for line in r:
            if case_id is not None and case_id != line["case_id"]:
                raise ValueError("multiple case IDs in samplesheet")
            case_id = line["case_id"]
        if case_id is None:
            raise ValueError("case ID not found")
        if "-" in case_id:
            raise ValueError(f"case IDs cannot contain dashes: {case_id}")
        return case_id

    def _build_sample_entries(self, samples):
        sample_entries = []

        for sample in samples:
            sample_name = (
                sample.get("sample_id")
                or sample.get("name")
                or sample.get("sample")
            )

            if not sample_name:
                raise ValueError("Could not determine sample name from sample payload")

            entry = {
                "sample_id": sample_name,
                "bam_file": self._sample_file(self.pipeline_dir, "bam", sample_name),
                "d4_file": self._sample_file(self.pipeline_dir, "d4", sample_name),
                "sex": sample.get("sex"),
                "phenotype": sample.get("phenotype"),
                "chromograph_autozygous": self._sample_file(self.pipeline_dir, "chromograph_autozygous", sample_name),
                "chromograph_coverage": self._sample_file(self.pipeline_dir, "chromograph_coverage", sample_name),
            }
            sample_entries.append(entry)

        return sample_entries
    
    def _case_file(self, dir: Path, filetype: str, case_id: str | None) -> Optional[str]:
        if filetype not in PIPELINE_CASE_FILES:
            raise KeyError("invalid case file type: {filetype}")
        p = dir / PIPELINE_OUTPUT_DIR / PIPELINE_CASE_FILES[filetype].format(case=case_id)
        log.debug(f"asset case={case_id} asset={filetype} path={p} exists={p.exists()}")
        if not p.exists():
            return None
        return str(p)
    
    def _sample_file(self, dir: Path, filetype: str, sample: str) -> Optional[str]:
        if filetype not in PIPELINE_SAMPLE_FILES:
            raise KeyError("invalid sample file type: {filetype}")
        p = dir / PIPELINE_OUTPUT_DIR / PIPELINE_SAMPLE_FILES[filetype]["path"].format(sample=sample)
        log.debug(f"asset sample={sample} asset={filetype} path={p} exists={p.exists()}")
        if not PIPELINE_SAMPLE_FILES[filetype]["is_prefix"] and not p.exists():
            return None
        return str(p)
    
    def _get_analysis_date(self, dir: Path):
        multiqc = self._case_file(dir, "multiqc", None)
        if multiqc is None:
            return datetime.now()
        info = os.stat(multiqc)
        analysis_date = datetime.fromtimestamp(info.st_mtime)
        log.debug(f"analysis date: {analysis_date}")
        return analysis_date


    def _write_yaml(self, config, output_dir):
        outdir = Path(output_dir)
        outdir.mkdir(parents=True, exist_ok=True)

        path = outdir / f"scout_load_config.yaml"
        with open(path, "w") as fh:
            yaml.safe_dump(config, fh, sort_keys=False)

        return str(path)