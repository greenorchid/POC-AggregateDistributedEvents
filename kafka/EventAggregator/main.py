import asyncio
from typing import AsyncIterable
import faust
from os import environ
from datetime import datetime, timedelta
import logging

KAFKA_SERVER = environ.get('KAFKA_SERVER','localhost:9094')
FAUST_CLIENT_ID = "EventAggregator"
KAFKA_PARTITIONS = int(environ.get('KAFKA_PARTITIONS',3)) #must match number of partitions for each data events topic

FAUST_AGGREGATOR_PARTITIONS = int(environ.get('FAUST_AGGREGATOR_PARTITIONS',2)) #divided across each faust worker
TTL_FOR_EVENTS = int(environ.get('AGGREGATE_EVENTS_TTL',2)) #minutes to wait for aggregation of all event types

log = logging.getLogger(__name__)

app = faust.App(
    FAUST_CLIENT_ID,
    broker=f'kafka://{KAFKA_SERVER}',
    value_serializer='json',
)

class EventData(faust.Record, serializer='json'):
    transaction: str
    log: str
    
class AggregatedEventData(faust.Record, serializer='json'):
    transaction: str
    events_typea_value: str | dict = None
    events_typeb_value: str | dict = None
    events_typec_value: str | dict = None
    events_typed_value: str | dict = None
    events_typee_value: str | dict = None
    completed: bool = False
    

# Define a Faust Table to store the state
combined_stream_table = app.Table('combined_stream_table', default=dict, partitions=FAUST_AGGREGATOR_PARTITIONS)

# Define a Faust Table to track completed events from each stream
completed_status_table = app.Table('completed_status_table', default=dict, partitions=FAUST_AGGREGATOR_PARTITIONS)

# Define an ephemeral Faust Channel to track events that have expired
expired_status_channel = app.channel()

# Define a Faust Topic as a destination for completed/aggregated events
aggregated_events_topic = app.topic('Events_Aggregated', value_type=AggregatedEventData)

# Define a Faust Topic as a destination for events that have passed TTL
expired_events_topic = app.topic('Events_Expired', value_type=AggregatedEventData)

# Define Faust topics for each stream
event_topics = ['Events-TypeA','Events-TypeB','Events-TypeC','Events-TypeD','Events-TypeE']


# programmatic creation fo agent adapted from https://github.com/robinhood/faust/issues/300#issuecomment-525531059
# Alternative explicit agent creation left commented below
def create_agent(start_topic: str) -> faust.agents.Agent:
    """ 
         creation of a single agent.
         ...thanks to closures
 
         `start_topic`:  str
             Just a string that you can use in other functions
             to figure out how messages in that topic can be
             transformed
    """

    async def agent(events: AsyncIterable[faust.events.Event]):
        """
             send messages from one topic to another
             ...and possibly transform the message before sending
        """
        # purely for co-partitioning POC tests (see EventAggregator.partition_assignment()) we'll group
        # by the transaction key across multiple partitions. In production we would not do this.
        async for event in events.group_by(EventData.transaction, partitions=FAUST_AGGREGATOR_PARTITIONS):
            key = event.transaction
            value = event.log

            aggregate_stream_value(key, f"{start_topic.lower()}_value", value)
            await check_and_dispatch_all_completed_events(key)
            
    return agent

def agents(event_topics: list[str]) -> list[faust.agents.Agent]:
    """
        configuration of multiple agents
        ....again, thanks to closures
    """
    agents = []
    for topic_name in event_topics:
        """ `topic`:  app.topic instance """
        agents.append(
            (create_agent(topic_name),
             topic_name)
        )
    return agents

agents_dict: dict[faust.agents.Agent] = {}
topics_dict:dict[faust.topics.Topic] = {}
def attach_agent(agent: faust.agents.Agent, topic: faust.topics.Topic):
    """ Attach the agent to the Faust app """
    
    #this assumes each topic has the same number of KAFKA_PARTITIONS
    topics_dict[topic] = app.topic(topic, partitions=KAFKA_PARTITIONS, value_type=EventData)
    agents_dict[topic] = app.agent(channel=topics_dict[topic], name=f"{topic}_agent")(agent)
    agents_dict[topic].start()

# Define Faust agent to process events from each stream
for agent,topic in agents(event_topics):
    attach_agent(agent, topic)
    
"""
#ALT: explicitly decorate functions with agent, example below:
event_type_A_topic = app.topic("Events-TypeA", partitions=KAFKA_PARTITIONS, value_type=EventData)

@app.agent(event_type_A_topic)
async def process_event_type_A(event_type_A_events):
    # purely for co-partitioning POC tests (see EventAggregator.partition_assignment()) we'll group
    # by the transaction key across multiple partitions. In production we would not do this.
    async for event in event_type_A_events.group_by(
            EventData.transaction,
            partitions=FAUST_AGGREGATOR_PARTITIONS
        ):
        key = event.transaction
        value = event.log

        # Store the value in the table
        aggregate_stream_value(key, 'events-typea_value', value)
        await check_and_dispatch_all_completed_events(key)
"""
   
def aggregate_stream_value(key: str, event_type: str, value: str):
    # Update table with event_type value
    combined_stream_value = completed_status_table[key]
    combined_stream_value[event_type] = value
    # update timestamp
    combined_stream_value['timestamp'] = datetime.now().isoformat()
    completed_status_table[key] = combined_stream_value

async def check_and_dispatch_all_completed_events(key: str):
    #checks if all events belonging to key are in completed_status_table
    combined_stream_value = completed_status_table[key]
    all_event_type_values = [f"{f.lower()}_value" for f in event_topics]
    
    if all (data_type in combined_stream_value for data_type in all_event_type_values) \
        and not combined_stream_value.get('completed', False):
            log.info(f"Combined Values for {key}: {combined_stream_value}")
            combined_events = {**combined_stream_value, 'transaction': key, 'completed': True}
            await aggregated_events_topic.send(key=key, value=combined_events)
            #mark event as completed
            completed_status_table[key] = combined_events

@app.agent(expired_status_channel)
async def update_status_table_for_expired_events(events):
    # we can only update table from within a stream operation
    async for event in events:
        completed_status_table[event['transaction']] = {**event}

async def find_expired_events():
    while True:
        for event in completed_status_table:
            combined_stream_value = completed_status_table[event]
            current_time = datetime.now()
            event_timestamp = datetime.fromisoformat(completed_status_table[event]['timestamp'])
            is_completed = bool(completed_status_table[event].get('completed', False))
            if (current_time - event_timestamp) > timedelta(minutes=TTL_FOR_EVENTS) and not is_completed:
                log.warn(f"EXPIRED transaction: {event}")
                combined_events = {**combined_stream_value, 'transaction': event, 'completed': True}
                await expired_events_topic.send(key=event, value=combined_events)
                # add event to channel so we can update the completed_status_table
                await expired_status_channel.send(key=event, value=combined_events)
                
        # Sleep for a certain interval before checking again
        await asyncio.sleep(10)

# Start the find_expired_events task in the background
app.loop.create_task(find_expired_events())


#initialize topics if required
@app.command()
async def init():
    async with app.producer:
        await aggregated_events_topic.changelog_topic.maybe_declare()
        await expired_events_topic.changelog_topic.maybe_declare()
        await expired_status_channel.maybe_declare()
        await asyncio.sleep(1)


if __name__ == '__main__':
    app.main()
