from st2tests.base import BaseActionTestCase

from parse_scout_panels import ParseScoutPanels


class ParseScoutPanelsActionTestCase(BaseActionTestCase):
    action_cls = ParseScoutPanels

    def test_parse_scout_panels(self):
        panels_tsv = (
            "#panel_name\tversion\tnr_genes\thidden\tdate\n"
            "panel_1\t1.0\t36\tFalse\t2025-06-13\n"
            "panel_2\t1.0\t10\tTrue\t2025-12-19\n"
            "panel_3\t1.0\t10\tFalse\t2025-11-28\n"
        )

        action = self.get_action_instance()

        success, result = action.run(panels_tsv=panels_tsv)

        self.assertTrue(success, result)
        self.assertEqual(len(result), 3)

        self.assertEqual(
            result[0],
            {
                "panel_name": "panel_1",
                "version": "1.0",
                "nr_genes": 36,
                "hidden": False,
                "date": "2025-06-13",
            },
        )
