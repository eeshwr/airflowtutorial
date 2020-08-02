
from database import Database
import sqlalchemy as sa
import pandas as pd
from datetime import datetime, timedelta
import pendulum


class DailyReport:
    db = Database()
    engine = db.engine
    events = db.table('events')
    actions = db.table('actions')
    members = db.table('members')

    def process_actions(self, actions):
        index = 0
        private_time = []
        break_time = []
        while index < len(actions) - 1:
            first_action = actions[index]
            second_action = actions[index + 1]
            if (
                first_action["action_name"] == "private-time-start"
                and second_action["action_name"] == "private-time-stop"
            ):
                private_time.append(
                    (
                        second_action["action_timestamp"]
                        - first_action["action_timestamp"]
                    ).total_seconds()
                )
                index += 2
            elif (
                first_action["action_name"] == "break-start"
                and second_action["action_name"] == "break-stop"
            ):
                break_time.append(
                    (
                        second_action["action_timestamp"]
                        - first_action["action_timestamp"]
                    ).total_seconds()
                )
                index += 2
            index += 1
        return private_time, break_time

    def process_events(self, events, threshold):  # threshold is in minutes
        index = 0
        idle_time = []
        while index < len(events) - 1:
            time_diff = (
                events[index + 1]["event_time"] - events[index]["event_time"]
            ).total_seconds()
            if time_diff > (threshold * 60):  # convert to seconds
                idle_time.append(time_diff)
            index += 1
        return idle_time

    def create_daily_report(self):
        to_timestamp = datetime.now()
        from_timestamp = to_timestamp - timedelta(hours=12)

        # from_timestamp = pendulum.parse('2020-06-16 09:32:40.567141+00')
        # to_timestamp = pendulum.parse('2020-06-16 09:38:58.307141+00')

        members = self.get_members_for_daily_report(from_timestamp, to_timestamp)
        data = []
        for member in members:
            events = self.get_events_for_daily_report(
                from_timestamp, to_timestamp, member.member_id
            )
            actions = self.get_actions(member.member_id, from_timestamp, to_timestamp)
            private_time, break_time = self.process_actions(actions)
          
            idle_time = self.process_events(events, threshold=3)
            break_hours = sum(break_time) / 3600  # convert to hours
            private_hours = sum(private_time) / 3600
            office_hours = (to_timestamp - from_timestamp).total_seconds() / 3600
            idle_hours = sum(idle_time) / 3600
            active_hours = office_hours - (idle_hours + private_hours + break_hours)
            values = {
                "member_id": member.member_id,
                "workspace_id": member.workspace_id,
                "break_hours": break_hours,
                "private_hours": private_hours,
                "office_hours": office_hours,
                "idle_hours": idle_hours,
                "active_hours": active_hours,
                "start_time": from_timestamp,
                "end_time": to_timestamp,
            }
            data.append(values)
        df = pd.DataFrame.from_records(data)
        df.to_csv(
            'daily_report.csv',
            index=False,
            header=True)

    def generate_daily_report(self):
        to_timestamp = datetime.now()
        from_timestamp = to_timestamp - timedelta(hours=12)

        # from_timestamp = '2020-06-16 08:17:17.862045+00'
        # to_timestamp = pendulum.parse(from_timestamp) + timedelta(hours=12)

        df = self.read_actions(from_timestamp, to_timestamp)
        grouped = df.groupby(['member_id', 'workspace_id'])
        data = []
        for (member, workspace), actions in grouped:
            actions_dict = actions.to_dict(orient='record')
            in_index, clock_in_time = self.clock_in_time(actions_dict)
            out_index, clock_out_time = self.clock_out_time(actions_dict)

            if(out_index is None):
                out_index = len(actions_dict)-1
                clock_out_time = actions_dict[-1]['action_timestamp']
            
            events = self.get_events_for_daily_report(
                clock_in_time, clock_out_time, member
            )
            actions = actions_dict[in_index:out_index+1]
            private_time, break_time = self.process_actions(actions)
          
            idle_time = self.process_events(events, threshold=3)
            break_hours = sum(break_time) / 3600  # convert to hours
            private_hours = sum(private_time) / 3600
            office_hours = (clock_out_time - clock_in_time).total_seconds() / 3600
            idle_hours = sum(idle_time) / 3600
            active_hours = office_hours - (idle_hours + private_hours + break_hours)
            values = {
                "member_id": member,
                "workspace_id": workspace,
                "break_hours": break_hours,
                "private_hours": private_hours,
                "office_hours": office_hours,
                "idle_hours": idle_hours,
                "active_hours": active_hours,
                "start_time": from_timestamp,
                "end_time": to_timestamp,
            }
            data.append(values)
        df = pd.DataFrame.from_records(data)
        df.to_csv(
            'daily_report.csv',
            index=False,
            header=True)
    
    def get_members_for_daily_report(self, from_timestamp, to_timestamp):
        query = sa.select(
            [
                self.events.c.member_id,
                self.members.c.workspace_id,
            ]).distinct(self.events.c.member_id).select_from(
                self.events.join(
                    self.members,
                    self.events.c.member_id == self.members.c.id
                )
            )
        query = query.where(
            sa.and_(
                self.events.c.event_time > from_timestamp,
                self.events.c.event_time <= to_timestamp,
            )
        )
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            return results

    def get_events_for_daily_report(self, from_timestamp, to_timestamp, member):
        query = sa.select([self.events])
        query = query.where(
            sa.and_(
                self.events.c.event_time > from_timestamp,
                self.events.c.event_time <= to_timestamp,
                self.events.c.member_id == member,
            )
        )
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            return results

    def write_to_db(self):
        df = pd.read_csv('daily_report.csv')
        df.to_sql(
            'daily_report',
            con=self.engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )

    def get_actions(self, member, from_timestamp, to_timestamp):
        with self.engine.connect() as conn:
            query = sa.select([self.actions]).where(
                sa.and_(
                    self.actions.c.action_timestamp.between(from_timestamp, to_timestamp),
                    self.actions.c.member_id == member,
                )
            )
            results = conn.execute(query).fetchall()
            return results

    def read_actions(self, from_timestamp, to_timestamp):
        query = sa.select(
                    [
                        self.actions.c.action_name,
                        self.actions.c.member_id,
                        self.actions.c.action_timestamp,
                        self.members.c.workspace_id,
                    ]
                ).select_from(
                self.actions.join(
                        self.members,
                        self.actions.c.member_id == self.members.c.id
                    )
                )
        
        query = query.where(
                self.actions.c.action_timestamp.between(
                    from_timestamp, to_timestamp)
        )

        df = pd.read_sql_query(query, con=self.engine)
        return df

    def clock_in_time(self, actions):
        for index, action in enumerate(actions):
            if action['action_name'] == 'clock-in':
                return index, action['action_timestamp']

    def clock_out_time(self, actions):
        for index, action in enumerate(list(reversed(actions))):
            if action['action_name'] == 'clock-out':
                return len(actions) - 1 - index, action['action_timestamp']
