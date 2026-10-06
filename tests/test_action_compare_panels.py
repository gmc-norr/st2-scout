from st2tests.base import BaseActionTestCase

from compare_panels import ComparePanels


class ComparePanelsActionTestCase(BaseActionTestCase):
    action_cls = ComparePanels

    def test_compare_panels(self):
        scout_panels = [
            {
                "panel_name": "PANEL_A",
                "version": "1.0",
                "nr_genes": 3,
                "hidden": False,
                "date": "2025-06-13",
            },
            {
                "panel_name": "PANEL_B",
                "version": "1.0",
                "nr_genes": 2,
                "hidden": False,
                "date": "2025-06-13",
            },
        ]

        igene_panels = [
            {
                "id": "PANEL_A",
                "name": "Panel A",
                "genes": [
                    {"hgnc": "HGNC:1", "symbol": "GENE1"},
                    {"hgnc": "HGNC:2", "symbol": "GENE2"},
                    {"hgnc": "HGNC:3", "symbol": "GENE3"},
                ],
            },
            {
                "id": "PANEL_B",
                "name": "Panel B",
                "genes": [
                    {"hgnc": "HGNC:4", "symbol": "GENE4"},
                    {"hgnc": "HGNC:5", "symbol": "GENE5"},
                    {"hgnc": "HGNC:6", "symbol": "GENE6"},
                ],
            },
            {
                "id": "PANEL_C",
                "name": "Panel C",
                "genes": [
                    {"hgnc": "HGNC:7", "symbol": "GENE7"},
                ],
            },
        ]

        action = self.get_action_instance()

        success, result = action.run(
            scout_panels=scout_panels,
            igene_panels=igene_panels,
        )

        self.assertTrue(success)

        self.assertEqual(len(result["matching_panels"]), 1)
        self.assertEqual(len(result["gene_count_mismatches"]), 1)
        self.assertEqual(len(result["missing_in_scout"]), 1)

        matching = result["matching_panels"][0]

        self.assertEqual(matching["panel_name"], "PANEL_A")
        self.assertEqual(matching["igene_gene_count"], 3)
        self.assertEqual(matching["scout_gene_count"], 3)

        mismatch = result["gene_count_mismatches"][0]

        self.assertEqual(mismatch["panel_name"], "PANEL_B")
        self.assertEqual(mismatch["igene_gene_count"], 3)
        self.assertEqual(mismatch["scout_gene_count"], 2)
        self.assertEqual(mismatch["gene_count_difference"], 1)

        missing = result["missing_in_scout"][0]

        self.assertEqual(missing["panel_name"], "PANEL_C")
        self.assertEqual(missing["igene_name"], "Panel C")
        self.assertEqual(missing["igene_gene_count"], 1)

        self.assertEqual(
            result["summary"],
            {
                "igene_panel_count": 3,
                "scout_panel_count": 2,
                "matching_panel_count": 1,
                "gene_count_mismatch_count": 1,
                "missing_in_scout_count": 1,
            },
        )
