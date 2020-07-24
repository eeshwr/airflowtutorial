from sqlalchemy.dialects import postgresql
from .database import Database
import sqlalchemy as sa
from collections import defaultdict


class ActivityRecord:
    db = Database()
    engine = db.engine
    reporting_meta = db.table('reporting_meta')
    events = db.table('events')
    members = db.table('members')
    teams = db.teams('teams')
    workspace_application_catalog = db.table('workspace_application_catalog')
    application_catalog = db.table('application_catalog')
    activity_record = db.table('activity_record')

    def get_value(self, key):
        with self.engine.connect() as conn:
            cursor = conn.execute(
                sa.select([self.reporting_meta.c.value]).where(self.reporting_meta.c.key == key)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            return None

    def set_value(self, key, value):
        with self.engine.connect() as conn:
            insert_stmt = postgresql.insert(self.reporting_meta).values(key=key, value=value)
            conn.execute(
                insert_stmt.on_conflict_do_update(
                    constraint="uq_reporting_meta_key",
                    set_=dict(value=insert_stmt.excluded.value),
                    where=(self.reporting_meta.c.key == insert_stmt.excluded.key),
                )
            )

    def add_activity_record(self):
        from_timestamp = self.get_value("activity_record")
        members = self.get_members(from_timestamp)
        for member in members:
            events = self.get_events(from_timestamp, member.member_id)
            events_by_task_id = defaultdict(list)
            for items in events:
                events_by_task_id[items["task_id"]].append(
                    (
                        # items["id"],
                        items["data"],
                        items["event_time"],
                        items["workspace_id"],
                        items["team_id"],
                    )
                )

            for task_id, event_records in events_by_task_id.items():
                idle_hours_with_application = self.get_idle_hours(event_records, 3)
                for application, app_data in idle_hours_with_application.items():
                    values = {
                        "member_id": member.member_id,
                        "task_id": task_id,
                        "team_id": app_data[0]["team_id"],
                        "workspace_id": app_data[0]["workspace_id"],
                        "idle_hours": sum([item["idle_hours"] for item in app_data]),
                        "start_time": event_records[0][1],
                        "end_time": event_records[-1][1],
                        "application_name": app_data[0]["application_name"],
                        "window_title": app_data[0]["window_title"],
                        "productivity_flag": self.get_productivity_flag(
                            application, member.member_id
                        ),
                    }

                    with self.engine.connect() as conn:
                        conn.execute(self.activity_record.insert().values(values))
        self.set_value("activity_record", sa.func.now())

    def get_events(self, from_timestamp, member):
        query = (
            sa.select(
                [
                    # events.c.id,
                    self.events.c.task_id,
                    self.events.c.data,
                    self.events.c.event_time,
                    self.members.c.workspace_id,
                    self.teams.c.id.label("team_id"),
                ]
            )
            .select_from(
                self.events.join(self.members, self.events.c.member_id == self.members.c.id).join(
                    self.teams, self.members.c.workspace_id == self.teams.c.workspace_id
                )
            )
            .where(self.events.c.member_id == member)
        )
        if from_timestamp is not None:
            query = query.where(
                sa.and_(
                    self.events.c.event_time > from_timestamp,
                    self.events.c.event_time <= sa.func.now(),
                )
            )
        else:
            query = query.order_by(self.events.c.event_time).limit(100)
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            return results

    def get_idle_hours(self, events, idle_threshold):
        index = 0
        application_to_idle_hours = defaultdict(list)
        idle_time = []
        previous_application = None
        previous_window = None
        previous_event_time = None
        previous_team_id = None
        previous_workspace_id = None
        while True:
            data = events[index][0]
            application = data["application"]
            window = data["window_title"]
            if len(application) == 0 or len(window) == 0:
                index += 1
            else:
                break
        while index < len(events) - 1:
            data = events[index][0]
            workspace_id = events[index][2]
            team_id = events[index][3]
            application = data["application"]
            window = data["window_title"]
            event_time = events[index][1]
            if previous_application is None:
                previous_application = application
                previous_window = window
                previous_event_time = event_time
                previous_workspace_id = workspace_id
                previous_team_id = team_id
                index += 1
            else:
                if (len(application) == 0 or len(window) == 0) or (
                    application == previous_application and window == previous_window
                ):
                    time_diff = (event_time - previous_event_time).total_seconds()
                    if time_diff > (idle_threshold * 60):  # convert to seconds
                        idle_time.append(time_diff)
                    if len(application) != 0:
                        previous_window = window
                        previous_application = application

                    previous_event_time = event_time
                else:
                    application_to_idle_hours[previous_application].append(
                        {
                            "idle_hours": sum(idle_time) / 3600,
                            "team_id": previous_team_id,
                            "workspace_id": previous_workspace_id,
                            "application_name": previous_application,
                            "window_title": previous_window,
                        }
                    )
                    previous_application = application
                    previous_window = window
                    idle_time = []

                index += 1
        return application_to_idle_hours

    def get_members(self, from_timestamp):
        query = sa.select([self.events.c.member_id, self.events.c.event_time]).distinct(
            self.events.c.member_id
        )
        if from_timestamp is not None:
            query = query.where(
                sa.and_(
                    self.events.c.event_time > from_timestamp,
                    self.events.c.event_time <= sa.func.now(),
                )
            )
        else:
            query = query.order_by(self.events.c.member_id, self.events.c.event_time).limit(100)
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            return results

    def get_productivity_flag(self, application, _member):
        application_list = self.get_applications()
        workspace_app_list = {}
        if _member not in workspace_app_list:
            catalog = self.get_application_catalog_by_member(_member)
            workspace_app_list[_member] = catalog
        _app_name = removesuffix(application, "-exe")
        _this = workspace_app_list[_member]
        _flag = "Undefined"
        if _this and _app_name in application_list:
            _app_id = application_list[_app_name]
            if _app_id in _this["productive"]:
                _flag = "Productive"
            elif _app_id in _this["unproductive"]:
                _flag = "Unproductive"
            elif _app_id in _this["neutral"]:
                _flag = "Neutral"
        return _flag

    def get_application_catalog_by_member(self, member):
        with self.engine.connect() as conn:
            cursor = conn.execute(
                sa.select([self.workspace_application_catalog])
                .select_from(
                    self.members.join(
                        self.workspace_application_catalog,
                        self.members.c.workspace_id
                        == self.workspace_application_catalog.c.workspace_id,
                    )
                )
                .where(self.members.c.id == member)
            )
            return cursor.fetchone()

    def get_applications(self):
        with self.engine.connect() as conn:
            application_list = {
                rec["name"]: rec["id"]
                for rec in conn.execute(
                    sa.select([self.application_catalog.c.name, self.application_catalog.c.id])
                )
            }
            return application_list

    def get_workspaces(self):
        with self.engine.connect() as conn:
            workspaces_list = {
                rec["id"]: rec["workspace_id"]
                for rec in conn.execute(
                    sa.select([self.members.c.id, self.members.c.workspace_id])
                )
            }
            return workspaces_list


def removesuffix(string: str, suffix: str, /) -> str:
    if suffix and string.endswith(suffix):
        return string[: -len(suffix)]
    return string[:]
