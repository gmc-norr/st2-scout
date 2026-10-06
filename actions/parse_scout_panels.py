import csv
import io

from st2common.runners.base_action import Action


class ParseScoutPanels(Action):

    def run(self, panels_tsv):
        if panels_tsv is None or not panels_tsv.strip():
            raise ValueError("Scout panels TSV output is empty")

        stream = io.StringIO(panels_tsv)
        reader = csv.DictReader(stream, delimiter="\t")

        panels = []

        for row_number, row in enumerate(reader, start=2):
            panel = {
                self._clean_header(key): self._convert_value(value)
                for key, value in row.items()
                if key is not None
            }

            if not panel.get("panel_name"):
                raise ValueError(
                    "Missing panel_name on TSV row {}".format(row_number)
                )

            panels.append(panel)

        return panels
    
    def _clean_header(self, header):
        return header.strip().lstrip("#")


    def _convert_value(self, value):
        if value is None:
            return None

        value = value.strip()

        if value == "True":
            return True

        if value == "False":
            return False

        if value.isdigit():
            return int(value)

        return value