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
            self.sf_id = sf_id

        self.fw_costs = self.metadata['costs'] if 'costs' in self.metadata else {}

        super().__init__(fireworks, links_dict, name, metadata, created_on, updated_on, fw_states)

    def _reassign_ids(self, old_new):
        """
        Internal method to reassign Firework ids, e.g. due to database insertion.

        Args:
                old_new (dict)
        """

        super()._reassign_ids(old_new)

        # update fw costs
        new_fw_costs = {}
        for (fwid, cost) in self.fw_costs.items():
            new_fw_costs[str(old_new.get(int(fwid), int(fwid)))] = cost
        self.fw_costs = new_fw_costs
        self.metadata['costs'] = self.fw_costs

    def to_db_dict(self):
        m_dict = super().to_db_dict()
        m_dict['sf_id'] = self.sf_id
        return m_dict

    @classmethod
    def from_Firework(cls, fw, name=None, metadata=None, sf_id=None):
        """
        Return Workflow from the given Firework.

        Args:
            fw (Firework)
            name (str): New workflow's name. if not provided, the firework name is used
            sf_id (int): Id of the Swarmflow
            metadata (dict): New workflow's metadata.

        Returns:
            Swarmflow
        """

        name = name if name else fw.name
        return Swarmflow([fw], name=name, metadata=metadata, created_on=fw.created_on,
                         updated_on=fw.updated_on, sf_id=sf_id)
