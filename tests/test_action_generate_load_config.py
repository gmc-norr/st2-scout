import sys
import tempfile
from pathlib import Path
from st2tests.base import BaseActionTestCase

from generate_load_config import GenerateLoadConfigAction, PIPELINE_RD


def touch(path: Path):
    path.touch()

class GenerateLoadConfigActionTestCase(BaseActionTestCase):
    action_cls = GenerateLoadConfigAction

    def test_generate_load_config_basic(self):
        case_id = "FAM001"
        sample_id = "SAMPLE1"

        with tempfile.TemporaryDirectory() as d:
            tmp_path = Path(d)

            # mock case files 
            case_paths = [
                tmp_path / f"{case_id}_snv_ranked_clinical.vcf.gz",
                tmp_path / "multiqc_report.html",
            ]
            for p in case_paths:
                touch(p)
            case_files = [{"path": str(p)} for p in case_paths]

            # mock sample files
            sample_paths = [
                tmp_path / f"{sample_id}_sorted_md.bam",
                tmp_path / f"{sample_id}_mosdepth.per-base.d4",
                tmp_path / f"{sample_id}_rhocallviz_chr_1.png",
            ]
            for p in sample_paths:
                touch(p)
            sample_files = {sample_id: [{"path": str(p)} for p in sample_paths]}

            sample_info = {
                "SAMPLE1": {"Sex": "F"}
            }

            action = self.get_action_instance(
                config={
                    "owners_map": {PIPELINE_RD: "clingen-rd"},
                    "genomes_map": {PIPELINE_RD: "38"},
                    "rankmodel_map": {PIPELINE_RD: "rankmodel_v1.ii"},
                    "scout_case_file_suffixes": {"vcf_snv": "_snv_ranked_clinical.vcf.gz"},
                    "scout_sample_file_suffixes": {"alignment_path": "_sorted_md.bam", "d4_file": ".d4"},
                    "scout_chromograph_file_prefixes": {"autozygous": "_rhocallviz_chr"}
                }
            )


            success, result = action.run(
                sample_ids=[sample_id],
                case_id=case_id,
                case_name="Test family",
                sample_files=sample_files,
                sample_info=sample_info,
                case_files=case_files,
                pipeline=PIPELINE_RD,
                igene_panels=["CARDIO_PAN_WGS_v1.0"],
            )

            self.assertTrue(success)

            # --- case-level assertions ---
            self.assertEqual(result["family"], case_id)
            self.assertEqual(result["owner"], "clingen-rd")
            self.assertEqual(result["human_genome_build"], "38")
            self.assertIn("analysis_date", result)
            self.assertIn("gene_panels", result)
            self.assertIn("default_gene_panels", result)

            # --- sample-level assertions ---
            self.assertEqual(len(result["samples"]), 1)
            self.assertEqual(result["samples"][0]["sex"], "female")
            sample = result["samples"][0]

            self.assertIn("alignment_path", sample)
            self.assertIn("d4_file", sample)
            self.assertIn("chromograph_images", sample)

