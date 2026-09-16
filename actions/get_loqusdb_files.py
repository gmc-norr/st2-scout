from pathlib import Path

from st2common.runners.base_action import Action

import logging

log = logging.getLogger(__name__)

class GetLoqusdbFilesAction(Action):
    """
    Get name of the loqusdb config to use mounted within the docker container,
    depending on what pipeline is run
    """

    def run(self, pipeline: str, analysis_dir: str, case_id:str):

        try:
            loqusdb_config = self.config["loqusdb_config_map"][pipeline]

            loqusdb_vcf = self._get_vcf(pipeline, analysis_dir, case_id)
            loqusdb_ped = self._get_ped(pipeline, analysis_dir, case_id)
            return  (
                True,
                {
                    "loqusdb_config": loqusdb_config,
                    "loqusdb_ped": loqusdb_ped,
                    "loqusdb_vcf": loqusdb_vcf
                },
            )
        
        except Exception as e:
            return (False, {"error": str(e)})

    def _get_ped(self, pipeline:str, sample_id:str, case_id: str):

        ped_pattern = self.config["loqusdb_ped_pattern"][pipeline]

        if ped_pattern == "":
            return ""
        ped_path = Path(ped_pattern.replace('case_id', case_id).replace('sample_id', sample_id))

        if not ped_path.exists():
            raise FileNotFoundError(f"ped path {ped_path} does not exist")
            
        return str(ped_path)

    def _get_vcf(self, pipeline:str, sample_id: str, case_id: str):
        vcf_pattern = self.config["loqusdb_vcf_pattern"][pipeline]

        vcf_path = Path(vcf_pattern.replace('case_id', case_id).replace('sample_id', sample_id))

        if not vcf_path.exists():
            raise FileNotFoundError(f"vcf path {vcf_path} does not exist")
            
        return str(vcf_path)

                