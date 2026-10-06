from st2common.runners.base_action import Action


class ComparePanels(Action):

    def run(self, scout_panels, igene_panels):
        try:
            if not isinstance(scout_panels, list):
                raise TypeError(
                    f"scout_panels must be a list, "
                    f"got {type(scout_panels).__name__}"
                )

            if not isinstance(igene_panels, list):
                raise TypeError(
                    f"igene_panels must be a list, "
                    f"got {type(igene_panels).__name__}"
                )

            scout_by_name = self._index_scout_panels(scout_panels)

            matching_panels = []
            gene_count_mismatches = []
            missing_in_scout = []

            for igene_panel in igene_panels:
                igene_id = igene_panel.get("id")

                if igene_id is None:
                    raise ValueError(
                        f"iGene panel is missing required field 'id': "
                        f"{igene_panel}"
                    )
                # Skip if not WGS or GMS560 panel
                if ("_PAN_WGS_" not in igene_id
                    and "_PAN_GMS560_" not in igene_id
                ):
                    continue

                igene_genes = igene_panel.get("genes") or []
                igene_gene_count = len(igene_genes)

                # Skip if panel has no genes in igene
                if igene_gene_count == 0:
                    continue

                scout_panel = scout_by_name.get(igene_id)

                if scout_panel is None:
                    missing_in_scout.append({
                        "panel_name": igene_id,
                        "igene_name": igene_panel.get("name"),
                        "igene_gene_count": igene_gene_count,
                    })
                    continue

                scout_gene_count = scout_panel.get("nr_genes")

                comparison = {
                    "panel_name": igene_id,
                    "igene_name": igene_panel.get("name"),
                    "igene_gene_count": igene_gene_count,
                    "scout_gene_count": scout_gene_count,
                }

                if scout_gene_count == igene_gene_count:
                    matching_panels.append(comparison)
                else:
                    comparison["gene_count_difference"] = (
                        igene_gene_count - scout_gene_count
                        if isinstance(scout_gene_count, int)
                        else None
                    )
                    gene_count_mismatches.append(comparison)

            result = {
                "matching_panels": matching_panels,
                "gene_count_mismatches": gene_count_mismatches,
                "missing_in_scout": missing_in_scout,
                "summary": {
                    "igene_panel_count": len(igene_panels),
                    "scout_panel_count": len(scout_panels),
                    "matching_panel_count": len(matching_panels),
                    "gene_count_mismatch_count": len(gene_count_mismatches),
                    "missing_in_scout_count": len(missing_in_scout),
                },
            }

            return (True, result)

        except Exception as error:
            return (False, {"error": str(error)})

    def _index_scout_panels(self, scout_panels):
        scout_by_name = {}

        for panel in scout_panels:
            panel_name = panel.get("panel_name")

            if not panel_name:
                raise ValueError(
                    f"Scout panel is missing required field "
                    f"'panel_name': {panel!r}"
                )

            if panel_name in scout_by_name:
                raise ValueError(
                    f"Duplicate Scout panel_name: {panel_name!r}"
                )

            scout_by_name[panel_name] = panel

        return scout_by_name