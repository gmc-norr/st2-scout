import re

from st2common.runners.base_action import Action

import logging

log = logging.getLogger(__name__)

class ComparePanels(Action):
    """Action for creating load configs for scout"""

    def run(
        self,
        igene_panels: dict,
        scout_panels: str,
        case_name: str,
        sample_files: dict,
        sample_info: dict,
        case_files: list,
        pipeline: str,
        igene_panels: list,
        scout_specifics: dict,
    ):