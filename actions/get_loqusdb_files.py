from pathlib import Path

from st2common.runners.base_action import Action


PIPELINE_RD = "nf-core/raredisease"
PIPELINE_GMS = "genomic-medicine-sweden/Twist_Solid"


class GetLoqusdbFilesAction(Action):
    """
    Get name of the loqusdb config to use mounted within the docker container,
    depending on what pipeline is run
    """

    def run(self, pipeline: str, analysis_dir: str, sample_id: str, case_id: str):
        PIPELINE_TO_LOQUSDB_SPECIFICS = {
            PIPELINE_RD: {
                "loqusdb_config": "/home/worker/config/loqusdb_rd.yaml",
                "loqusdb_ped": f"{analysis_dir}/raredisease_results/pedigree/{case_id}.ped",
                "loqusdb_vcf": f"{analysis_dir}/raredisease_results/call_snv/genome/{case_id}_snv.vcf.gz"
            },
            PIPELINE_GMS: {
                "loqusdb_config": "/home/worker/config/loqusdb_somatic.yaml",
                "loqusdb_ped": "",
                "loqusdb_vcf": f"{analysis_dir}/results/dna/{sample_id}_T/additional_files/vcf/{sample_id}"
                "_T.annotated.exon_only.filter.soft_filter.vcf"
            }
        }
        try:
            specifics = PIPELINE_TO_LOQUSDB_SPECIFICS[pipeline]
        except KeyError as e:
            raise e(f"Unknown pipeline: {pipeline}")

        if not Path(specifics['loqusdb_vcf']).exists():
            raise FileNotFoundError(f"vcf path {specifics['loqusdb_vcf']} does not exist")
        if specifics["loqusdb_ped"] != "":
            if not Path(specifics['loqusdb_ped']).exists():
                raise FileNotFoundError(f"vcf path {specifics['loqusdb_ped']} does not exist")
        return  (True, specifics)
                