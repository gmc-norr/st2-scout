from pathlib import Path
import yaml

try:
    from st2common.runners.base_action import Action
except ImportError:

    class Action:
        def __init__(self, config=None):
            self.config = config


import logging

log = logging.getLogger(__name__)


class WriteLoadConfigAction(Action):
    """
    Write a Scout load config to YAML.
    """

    def run(
        self,
        load_config: dict,
        output_path: str,
        overwrite: bool = False,
    ):

        try:
            path = Path(output_path)

            if path.exists() and not overwrite:
                raise FileExistsError(
                    f"Config already exists: {path} (set overwrite=true to replace)"
                )

            parent_dir = path.parent
            if not parent_dir.exists() and parent_dir.is_dir():
                raise FileNotFoundError(f"{parent_dir} does not exist")

            with path.open("w") as fh:
                yaml.safe_dump(
                    config,
                    fh,
                    sort_keys=False,
                )

            log.info(f"Wrote Scout load config to {path}")

            return (
                True,
                {
                    "path": str(path),
                    "exists": path.exists(),
                },
            )

        except Exception as e:
            return (False, {"error": str(e)})
