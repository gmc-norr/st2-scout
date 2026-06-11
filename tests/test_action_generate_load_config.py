import sys
import tempfile
from pathlib import Path
from st2tests.base import BaseActionTestCase

from generate_load_config import GenerateLoadConfigAction
from get_pipeline_specific_scout_values import PIPELINE_TO_SCOUT_SPECIFICS

def touch(path: Path):
    path.touch()

class GenerateLoadConfigActionTestCase(BaseActionTestCase):
    action_cls = GenerateLoadConfigAction

    
    def _mock_files(self, tmp_path: Path, pipeline: str, case_id: str, sample_id: str):

        if pipeline == "nf-core/raredisease":
            case_paths = [
                tmp_path / f"{case_id}_snv_ranked_clinical.vcf.gz",
                tmp_path / f"{case_id}_snv_ranked_research.vcf.gz",
                tmp_path / f"{case_id}.peddy.ped",
                tmp_path / f"{case_id}.sex_check.csv",
                tmp_path / f"{case_id}.ped_check.csv",
                tmp_path / "multiqc_report.html",
                tmp_path / f"{case_id}_smncopynumbercaller.tsv",
            ]

            sample_paths = [
                tmp_path / f"{sample_id}_sorted_md.bam",
                tmp_path / f"{sample_id}_mosdepth.per-base.d4",
                tmp_path / f"{sample_id}_rhocallviz_chr_1.png",
            ]

        elif pipeline == "genomic-medicine-sweden/Twist_Solid":
            case_paths = [
                tmp_path / f"{case_id}.annotated.genmod.vcf.gz",
                tmp_path / f"{case_id}.general_report.html",
                tmp_path / f"{case_id}.pathology_purecn.cnv.html",
            ]

            sample_paths = [
                tmp_path / f"{sample_id}_T.bam",
                tmp_path / f"{sample_id}.coverage.d4",
            ]

        else:
            raise ValueError(f"Unknown pipeline: {pipeline}")

        for p in case_paths:
            touch(p)
        case_files = [{"path": str(p)} for p in case_paths]

        for p in sample_paths:
            touch(p)
        sample_files = {sample_id: [{"path": str(p)} for p in sample_paths]}

        return case_files, sample_files


    def test_generate_load_config_basic(self):
        case_id = "FAM001"
        sample_id = "SAMPLE1"
        pipeline = "nf-core/raredisease"
        sample_info = {
            "SAMPLE1": {"Sex": "F"}
        }

        with tempfile.TemporaryDirectory() as d:
            tmp_path = Path(d)
            
            case_files, sample_files = self._mock_files(
                tmp_path=tmp_path,
                pipeline=pipeline,
                case_id=case_id,
                sample_id=sample_id,
            )


            action = self.get_action_instance()

            success, result = action.run(
                sample_ids=[sample_id],
                case_id=case_id,
                case_name="Test family",
                sample_files=sample_files,
                sample_info=sample_info,
                case_files=case_files,
                pipeline=pipeline,
                igene_panels=["CARDIO_PAN_WGS_v1.0", "HTAD_PAN_WGS_v1.0"],
                scout_specifics=PIPELINE_TO_SCOUT_SPECIFICS[pipeline]
            )

            self.assertTrue(success, result)
            print(result)

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

        # Test for twist_solid
        case_id = "CASE1"
        sample_id = "Sample1"
        pipeline = "genomic-medicine-sweden/Twist_Solid"
        sample_info = {
            "Sample1": {"Sex": "M"}
        }

        with tempfile.TemporaryDirectory() as d:
            tmp_path = Path(d)
            
            case_files, sample_files = self._mock_files(
                tmp_path=tmp_path,
                pipeline=pipeline,
                case_id=case_id,
                sample_id=sample_id,
            )


            action = self.get_action_instance()

            success, result = action.run(
                sample_ids=[sample_id],
                case_id=case_id,
                case_name="Test family",
                sample_files=sample_files,
                sample_info=sample_info,
                case_files=case_files,
                pipeline=pipeline,
                igene_panels=["CANCER_PAN_GMS560_v1.0"],
                scout_specifics=PIPELINE_TO_SCOUT_SPECIFICS[pipeline]
            )

            self.assertTrue(success, result)
            print(result)

            # --- case-level assertions ---
            self.assertEqual(result["family"], case_id)
            self.assertEqual(result["owner"], "clingen-solid")
            self.assertEqual(result["human_genome_build"], "37")
            self.assertIn("analysis_date", result)
            self.assertIn("gene_panels", result)
            self.assertEqual(result["default_gene_panels"], ["cancer"])

            # --- sample-level assertions ---
            self.assertEqual(len(result["samples"]), 1)
            self.assertEqual(result["samples"][0]["sex"], "male")
            sample = result["samples"][0]

            self.assertIn("alignment_path", sample)
            self.assertIn("d4_file", sample)


            

