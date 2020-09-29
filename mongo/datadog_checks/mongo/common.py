# Source
SOURCE_TYPE_NAME = 'mongodb'

# Service check
SERVICE_CHECK_NAME = 'mongodb.can_connect'

# Replication states
"""
MongoDB replica set states, as documented at
https://docs.mongodb.org/manual/reference/replica-states/
"""
REPLSET_MEMBER_STATES = {
    0: ('STARTUP', 'Starting Up'),
    1: ('PRIMARY', 'Primary'),
    2: ('SECONDARY', 'Secondary'),
    3: ('RECOVERING', 'Recovering'),
    4: ('Fatal', 'Fatal'),  # MongoDB docs don't list this state
    5: ('STARTUP2', 'Starting up (forking threads)'),
    6: ('UNKNOWN', 'Unknown to this replset member'),
    7: ('ARBITER', 'Arbiter'),
    8: ('DOWN', 'Down'),
    9: ('ROLLBACK', 'Rollback'),
    10: ('REMOVED', 'Removed'),
}

DEFAULT_TIMEOUT = 30
ALLOWED_CUSTOM_METRICS_TYPES = ['gauge', 'rate', 'count', 'monotonic_count']
ALLOWED_CUSTOM_QUERIES_COMMANDS = ['aggregate', 'count', 'find']


def get_state_name(state):
    """Maps a mongod node state id to a human readable string."""
    if state in REPLSET_MEMBER_STATES:
        return REPLSET_MEMBER_STATES[state][0]
    else:
        return 'UNKNOWN'


def get_long_state_name(state):
    """Maps a mongod node state id to a human readable string."""
    if state in REPLSET_MEMBER_STATES:
        return REPLSET_MEMBER_STATES[state][1]
    else:
        return 'Replset state %d is unknown to the Datadog agent' % state


class DeploymentType(object):
    def __eq__(self, other):
        raise NotImplementedError

    def __ne__(self, other):
        """Overrides the default implementation (unnecessary in Python 3)"""
        return not self.__eq__(other)

    def is_principal(self):
        """Whether or not this node can be used to collect data from collections. Should only
        be True for one node in the cluster."""
        raise NotImplementedError

    def get_available_metrics(self):
        # TODO: Use this method to know what metrics to collect based on the deployment type.
        raise NotImplementedError


class MongosDeploymentType(DeploymentType):
    def __eq__(self, other):
        return type(other) is MongosDeploymentType

    def is_principal(self):
        # A mongos has full visibility on the data, Datadog agents should only communicate
        # with one mongos.
        return True

    def get_available_metrics(self):
        return None


class ReplicaSetDeploymentType(DeploymentType):
    def __init__(self, replset_get_status_payload, in_shard=False):
        self.replset_name = replset_get_status_payload['set']
        self.replset_state = replset_get_status_payload['myState']
        self.replset_state_name = get_state_name(replset_get_status_payload['myState']).lower()
        self.in_shard = in_shard
        self.is_primary = replset_get_status_payload['myState'] == 1

    def __eq__(self, other):
        if not type(other) is ReplicaSetDeploymentType:
            return False

        # Warning, fields of thos class should all be comparable.
        # TODO: Test me
        return self.__dict__ == other.__dict__

    def is_principal(self):
        # There is only ever one primary node in a replica set.
        # In case sharding is disabled, the primary can be considered the master.
        return not self.in_shard and self.replset_state == 1

    def get_available_metrics(self):
        return None


class StandaloneDeploymentType(DeploymentType):
    def __eq__(self, other):
        return type(other) is StandaloneDeploymentType

    def is_principal(self):
        # A standalone always have full visibility.
        return True

    def get_available_metrics(self):
        return None
