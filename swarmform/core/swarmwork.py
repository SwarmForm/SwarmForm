from fireworks import Workflow, Firework


class SwarmFlow(Workflow):

    def __init__(self, fireworks, links_dict=None, name=None, metadata=None, created_on=None, updated_on=None,
                 fw_states=None, sf_id=None):
        """
        Args:
            fireworks ([Firework]): all FireWorks in this SwarmFlow.
            links_dict (dict): links between the FWs as (parent_id):[(child_id1, child_id2)]
            name (str): name of the SwarmFlow.
            metadata (dict): metadata for this SwarmFlow.
            created_on (datetime): time of creation
            updated_on (datetime): time of update
            fw_states (dict): leave this alone unless you are purposefully creating a Lazy-style WF
            sf_id (int): SwarmFlow id
        """

        super().__init__(fireworks, links_dict, name, metadata, created_on, updated_on, fw_states)

        if sf_id is not None:
            self.sf_id = sf_id

        self.fw_costs = self.metadata['costs'] if 'costs' in self.metadata else {}

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
    def from_dict(cls, m_dict):
        """
        Return Workflow from its dict representation.

        Args:
            m_dict (dict): either a Workflow dict or a Firework dict

        Returns:
            SwarmFlow
        """

        if 'fws' in m_dict:
            created_on = m_dict.get('created_on')
            updated_on = m_dict.get('updated_on')
            return SwarmFlow([Firework.from_dict(f) for f in m_dict['fws']],
                            Workflow.Links.from_dict(m_dict['links']), m_dict.get('name'),
                            m_dict['metadata'], created_on, updated_on)
        else:
            return SwarmFlow.from_Firework(Firework.from_dict(m_dict))

    @classmethod
    def from_Firework(cls, fw, name=None, metadata=None, sf_id=None):
        """
        Return SwarmFlow from the given Firework.

        Args:
            fw (Firework)
            name (str): New SwarmFlow's name. if not provided, the firework name is used
            sf_id (int): Id of the SwarmFlow
            metadata (dict): New SwarmFlow's metadata.

        Returns:
            SwarmFlow
        """

        name = name if name else fw.name
        return SwarmFlow([fw], name=name, metadata=metadata, created_on=fw.created_on,
                         updated_on=fw.updated_on, sf_id=sf_id)
