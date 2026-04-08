
import sys
from pathlib import Path
import pytest

PACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACK_ROOT))

from actions.generate_load_config import GenerateLoadConfigAction, PIPELINE_CASE_FILES, PIPELINE_SAMPLE_FILES, PIPELINE_OUTPUT_DIR

def touch(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def mock_case_files(base_dir: Path, case_id: str):
    pipeline_dir = base_dir / PIPELINE_OUTPUT_DIR

    for rel_path in PIPELINE_CASE_FILES.values():
        path = pipeline_dir / rel_path.format(case=case_id)
        touch(path)


def mock_sample_files(base_dir: Path, sample: str):
    pipeline_dir = base_dir / PIPELINE_OUTPUT_DIR

    for entry in PIPELINE_SAMPLE_FILES.values():
        rel_path = entry["path"].format(sample=sample)
        path = pipeline_dir / rel_path

        if entry["is_prefix"]:
            path.parent.mkdir(parents=True, exist_ok=True)
            # create at least one file matching the prefix
            (path.parent / (path.name + "_001.txt")).touch()
        else:
            touch(path)


@pytest.fixture
def pipeline_dir(tmp_path):
    case_id = "FAM001"
    sample_id = "SAMPLE1"

    pipeline_mock_dir = tmp_path / sample_id
    pipeline_mock_dir.mkdir(parents=True, exist_ok=True)

    # samplesheet.csv
    (pipeline_mock_dir / "samplesheet.csv").write_text(
        "case_id\nFAM001\n"
    )

    # mock pipeline outputs
    mock_case_files(pipeline_mock_dir, case_id)
    mock_sample_files(pipeline_mock_dir, sample_id)

    return pipeline_mock_dir


def test_generate_load_config_basic(pipeline_dir):
    action = GenerateLoadConfigAction(config={})

    samples = [
        {
            "sample_id": "SAMPLE1",
            "ParentSampleID": "PARENTID1",
            "sex": "male",
            "phenotype": "affected",
            "Panels": ["CARDIO_PAN_WGS_v1.0"],
        }
    ]

    success, result = action.run(
        samples=samples,
        pipeline_dir=str(pipeline_dir),
        pipeline="raredisease",
        panels=[],
        default_panels=[],
    )

    assert success is True
    assert result["generated"] is True
    assert Path(result["config_path"]).exists()

