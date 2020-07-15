import sqlalchemy as sa
from sqlalchemy import Table, MetaData, create_engine
from sqlalchemy.dialects import postgresql
from datetime import timedelta
import pendulum
engine = None
def connect():
    global engine
    engine = create_engine('postgresql://postgres:postgres@localhost:5433/timetracko')


def dispose():
    engine.dispose()

def get_start_end_date(from_date, duration_in_hours):
    from_timestamp = pendulum.parse(from_date)
    to_timestamp = from_timestamp + timedelta(
            hours=duration_in_hours
        )
    return from_timestamp, to_timestamp

def actions(member,from_timestamp, to_timestamp):
    metadata = MetaData(bind=None)
    actions= Table('actions', metadata, autoload = True, autoload_with = engine)
    query = sa.select([actions]).where(
                sa.and_(
                    actions.c.action_timestamp.between(from_timestamp, to_timestamp),
                    actions.c.member_id == member,
                )
            )
        #stmt = select([table]).where(table.c.action_timestamp.between('2020-06-16 09:32:40.567141+00', '2020-06-16 09:38:58.307141+00'))
    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
        return results

def events(member, from_timestamp, to_timestamp):
    metadata = MetaData(bind=None)
    events = Table('events', metadata, autoload = True, autoload_with = engine)
    #stmt = select([table]).where(table.c.event_time.between('2020-06-16 09:32:40.567141+00', '2020-06-16 09:38:58.307141+00'))
    query = sa.select([events]).where(
            sa.and_(
                events.c.event_time.between(from_timestamp, to_timestamp),
                events.c.member_id == member,
            )
        )

    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
        return results

def process_actions(actions):
    index = 0
    private_time=[]
    break_time=[]
    while index < len(actions)-1:
        first_action = actions[index]
        second_action = actions[index+1]
        if first_action.action_name == 'private-time-start' and second_action.action_name == 'private-time-stop':
            private_time.append((second_action.action_timestamp-first_action.action_timestamp).total_seconds())
            index += 2
        elif first_action.action_name == 'break-start' and second_action.action_name == 'break-stop':
            break_time.append((second_action.action_timestamp-first_action.action_timestamp).total_seconds())
            index += 2
        index += 1
    return private_time, break_time


def process_events(events, threshold):# threshold is in minutes
    index = 0
    idle_time = []
    while index < len(events)-1:
        time_diff = (events[index+1].event_time - events[index].event_time).total_seconds()/60 # convert to minutes
        if time_diff > threshold:
            idle_time.append(time_diff)
        index += 1
    return idle_time
def update_productivity_report(values):
    metadata = MetaData(bind=None)
    report = Table('productivity_report', metadata, autoload = True, autoload_with = engine)
    insert_stmt = postgresql.insert(report).values(values)
    with engine.connect() as conn:
        conn.execute(insert_stmt)
