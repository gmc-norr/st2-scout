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

            loqusdb_vcf = self._get_vcf(analysis_dir, case_id)
            loqusdb_ped = self._get_ped(analysis_dir, case_id)
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

    def _get_ped(self, analysis_dir: str, case_id: str):

        ped_analysis_subdir = self.config["loqusdb_ped"]["analysis_subdir"]
        ped_suffix = self.config["loqusdb_ped"]["suffix"]
        ped_path = Path(analysis_dir) / ped_analysis_subdir / f"{case_id}{ped_suffix}"

        if not ped_path.exists():
            raise FileNotFoundError(f"ped path {ped_path} does not exist")
            
        return str(ped_path)

    def _get_vcf(self, analysis_dir: str, case_id: str):
        vcf_analysis_subdir = self.config["loqusdb_vcf"]["analysis_subdir"]
        vcf_suffix = self.config["loqusdb_vcf"]["suffix"]
        vcf_path = Path(analysis_dir) / vcf_analysis_subdir / f"{case_id}{vcf_suffix}"

        if not vcf_path.exists():
            raise FileNotFoundError(f"vcf path {vcf_path} does not exist")
            
        return str(vcf_path)

                