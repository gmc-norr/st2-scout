from pathlib import Path

try:
    from st2common.runners.base_action import Action
except ImportError:

    class Action:
        def __init__(self, config=None):
            self.config = config


import logging

log = logging.getLogger(__name__)

class GetLoqusdbConfigAction(Action):
    """
    Get name of the loqusdb config to use mounted within the docker container,
    depending on what pipeline is run
    """

    def run(self, pipeline: str):

        try:
            loqusdb_config self.config["loqusdb_config_map"][pipeline]
            return  (
                True,
                {
                    "loqusdb_config": loqusdb_config
                },
            )
        
        except Exception as e:
            return (False, {"error": str(e)})


                