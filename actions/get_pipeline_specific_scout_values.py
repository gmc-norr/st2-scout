from typing import Optional

from st2common.runners.base_action import Action

PIPELINE_RD = "nf-core/raredisease"
PIPELINE_GMS = "genomic-medicine-sweden/Twist_Solid"

PIPELINE_TO_SCOUT_SPECIFICS = {
    PIPELINE_RD: {
        "owner": "clingen-rd",
        "genome": "38",
        "rankmodel": "https://raw.githubusercontent.com/gmc-norr/config-files/main/rankmodels/rare_disease_rank_model_0.1.ini",
        "analysis_type": "wgs",
        "track": "rare",
        "global_panels": ("PANELAPP-GREEN", ),
        "scout_case_file_suffixes": {
            "vcf_snv": "_snv_ranked_clinical.vcf.gz",
            "vcf_snv_research": "_snv_ranked_research.vcf.gz",
            "peddy_ped": ".peddy.ped",
            "peddy_sex": ".sex_check.csv",
            "peddy_check": ".ped_check.csv",
            "multiqc": "multiqc_report.html",
            "smn_tsv": "_smncopynumbercaller.tsv",
        },
        "scout_sample_file_suffixes": {
            "d4_file": "_mosdepth.per-base.d4",
            "alignment_path": "_sorted_md.bam",
        },
        "scout_chromograph_file_prefixes": {
            "autozygous": "_rhocallviz_chr",
            "coverage": "_tidditcov_chr",
        },
    },
    PIPELINE_GMS: {
        "owner": "clingen-solid",
        "genome": "37",
        "rankmodel": "https://raw.githubusercontent.com/gmc-norr/config-files/main/rankmodels/cancer_rank_model_0.1.ini",
        "analysis_type": "panel",
        "track": "cancer",
        "global_panels": ("PANELAPP-GREEN", "gms560_all_genes"),
        "scout_case_file_suffixes": {
            "vcf_cancer": ".annotated.genmod.vcf.gz",
            "multiqc": ".general_report.html",
            "cnv_report": ".pathology_purecn.cnv.html",
        },
        "scout_sample_file_suffixes": {
            "alignment_path": ".bam",
            "d4_file": ".coverage.d4",
        },
        "biomarker_file_suffixes": {
            "hrd": "pathology_purecn.scarhrd_cnvkit_score.txt",
            "msi": ".msisensor_pro.filtered.score.tsv",
            "tmb": ".TMB.txt",
        }
    } 
}

class GetPipelineSpecificScoutValues(Action):
    """Action for creating load configs for scout"""

    def run(self, pipeline: str):

        try:
            return PIPELINE_TO_SCOUT_SPECIFICS[pipeline]
        except KeyError as e:
            raise e(f"Unknown pipeline: {pipeline}")
