from fireworks import Workflow


class Swarmflow(Workflow):

    def __init__(self, fireworks, links_dict=None, name=None, metadata=None, created_on=None, updated_on=None,
                 fw_states=None, sf_id=None):
        """
                Args:
                    fireworks ([Firework]): all FireWorks in this workflow.
                    links_dict (dict): links between the FWs as (parent_id):[(child_id1, child_id2)]
                    name (str): name of workflow.
                    metadata (dict): metadata for this Workflow.
                    created_on (datetime): time of creation
                    updated_on (datetime): time of update
                    fw_states (dict): leave this alone unless you are purposefully creating a Lazy-style WF
                    sf_id (int): Swarmflow id
                """

        # TODO: sf_id must be provided to make sure there are no errors. When calling add_wf, sf_id is explicitly set
        #  as it doesn't use a Workflow object
        if sf_id is not None:
            self.wf_id = sf_id

        self.fw_costs = self.metadata['costs'] if 'costs' in self.metadata else {}

        super().__init__(fireworks, links_dict, name, metadata, created_on, updated_on, fw_states)



