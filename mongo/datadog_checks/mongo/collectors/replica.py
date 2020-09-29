import time

from six.moves.urllib.parse import urlsplit

from datadog_checks.mongo.collectors.base import MongoCollector
from datadog_checks.mongo.common import SOURCE_TYPE_NAME, get_long_state_name, get_state_name

try:
    import datadog_agent
except ImportError:
    from datadog_checks.base.stubs import datadog_agent


class ReplicaCollector(MongoCollector):
    """Collect replica set metrics by running the replSetGetStatus command. Also keep track of the previous node state
    in order to submit events on any status change.
    """

    def __init__(self, check, tags, is_primary=False):
        super(ReplicaCollector, self).__init__(check, "admin", tags)
        # Members' last replica set states
        self._last_states = {}
        self.is_primary = is_primary
        # Makes a reasonable hostname for a replset membership event to mention.
        uri = urlsplit(self.check.clean_server_name)
        if '@' in uri.netloc:
            self.hostname = uri.netloc.split('@')[1].split(':')[0]
        else:
            self.hostname = uri.netloc.split(':')[0]
        if self.hostname == 'localhost':
            self.hostname = datadog_agent.get_hostname()

    def _report_replica_set_states(self, members, replset_name):
        """
        Report the member's replica set state
        * Submit a service check.
        * Create an event on state change.
        """

        for member in members:
            # The id field cannot be changed for a given replica set member.
            member_id = member['_id']
            status_id = member['state']
            old_state = self._last_states.get(member_id)
            if not old_state:
                # First time the agent sees this replica set member.
                continue

            if old_state == status_id:
                continue
            old_state_str = get_state_name(old_state)
            status_str = get_state_name(status_id)
            status_long_str = get_long_state_name(status_id)
            node_hostname = member['name']

            msg_title = "{} is {} for {}".format(node_hostname, status_str, replset_name)
            msg = (
                "MongoDB {node} (_id: {id}, {uri}) just reported as {status} ({status_short}) "
                "for {replset_name}; it was {old_state} before.".format(
                    node=node_hostname,
                    id=member_id,
                    uri=self.check.clean_server_name,
                    status=status_long_str,
                    status_short=status_str,
                    replset_name=replset_name,
                    old_state=old_state_str,
                )
            )

            event_payload = {
                'timestamp': int(time.time()),
                'source_type_name': SOURCE_TYPE_NAME,
                'msg_title': msg_title,
                'msg_text': msg,
                'host': node_hostname,
                'tags': [
                    'action:mongo_replset_member_status_change',
                    'member_status:' + status_str,
                    'previous_member_status:' + old_state_str,
                    'replset:' + replset_name,
                ],
            }
            if node_hostname != 'localhost':
                # Do not submit events with a 'localhost' hostname.
                event_payload['host'] = node_hostname
            self.check.event(event_payload)

    def collect(self, client):
        db = client["admin"]
        status = db.command('replSetGetStatus')
        result = {}

        # Find nodes: master and current node (ourself)
        current = primary = None
        for member in status.get('members'):
            if member.get('self'):
                current = member
            if int(member.get('state')) == 1:
                primary = member

        # Compute a lag time
        if current is not None and primary is not None:
            if 'optimeDate' in primary and 'optimeDate' in current:
                lag = primary['optimeDate'] - current['optimeDate']
                result['replicationLag'] = lag.total_seconds()

        if current is not None:
            result['health'] = current['health']

        if current is not None:
            # We used to collect those with a new connection to the primary, this is not required.
            total = 0.0
            cfg = client['local']['system.replset'].find_one()
            for member in cfg.get('members'):
                total += member.get('votes', 1)
                if member['_id'] == current['_id']:
                    result['votes'] = member.get('votes', 1)
            result['voteFraction'] = result['votes'] / total

        result['state'] = status['myState']

        self._submit_payload({'replSet': result})
        if self.is_primary:
            replset_name = status['set']
            self._report_replica_set_states(status['members'], replset_name)

        self._last_states = {member['_id']: member['state'] for member in status['members']}
