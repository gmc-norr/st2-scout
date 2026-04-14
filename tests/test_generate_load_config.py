import sys
from pathlib import Path
import pytest

PACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACK_ROOT))

from actions.generate_load_config import GenerateLoadConfigAction, PIPELINE_RD


def touch(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


@pytest.fixture
def mock_files(tmp_path):
    case_id = "FAM001"
    sample_id = "SAMPLE1"

    # --- case files ---
    case_files = [
        tmp_path / f"{case_id}_snv_ranked_clinical.vcf.gz",
        tmp_path / "multiqc_report.html",
    ]

    for f in case_files:
        touch(f)

    case_files = [{"path": str(p)} for p in case_files]

    # --- sample files ---
    sample_files = [
        tmp_path / f"{sample_id}_sorted_md.bam",
        tmp_path / f"{sample_id}_mosdepth.per-base.d4",
        tmp_path / f"{sample_id}_rhocallviz_chr_1.png",
    ]

    for f in sample_files:
        touch(f)

    sample_files = {sample_id: [{"path": str(p)} for p in sample_files]}

    return case_id, sample_id, case_files, sample_files


def test_generate_load_config_basic(mock_files):
    case_id, sample_id, case_files, sample_files = mock_files

    action = GenerateLoadConfigAction(
        config={
            "owners_map": {
                PIPELINE_RD: "clingen-rd",
            },
            "genomes_map": {
                PIPELINE_RD: "38",
            },
        }
    )

    success, result = action.run(
        sample_ids=[sample_id],
        case_id=case_id,
        case_name="Test family",
        sample_files=sample_files,
        case_files=case_files,
        pipeline=PIPELINE_RD,
        igene_panels=["CARDIO_PAN_WGS_v1.0"],
    )

    assert success is True

    # --- case-level assertions ---
    assert result["family"] == case_id
    assert result["owner"] == "clingen-rd"
    assert result["human_genome_build"] == "38"
    assert "analysis_date" in result
    assert "gene_panels" in result
    assert "default_panels" in result

    # --- sample-level assertions ---
    assert len(result["samples"]) == 1
    sample = result["samples"][0]

    assert "alignment_path" in sample
    assert "d4_path" in sample
    assert "chromograph_images" in sample
