from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin.new_partitions import NewPartitions
from kafka.errors import KafkaError, InvalidPartitionsError
from os import environ
import asyncio
import random
import uuid
from lorem.text import TextLorem
from json import dumps

KAFKA_SERVER = [environ.get('KAFKA_SERVER','localhost:9094')]
KAFKA_VALUE_SERIALIZER = lambda x: dumps(x).encode('ascii')
KAFKA_KEY_SERIALIZER = lambda x: str.encode(x)
KAFKA_CLIENT_ID = "EventGenerator"
KAFKA_TOPIC_PREFIX = "Events-"
KAFKA_PARTITIONS = int(environ.get('KAFKA_PARTITIONS',3))

EVENT_DATA_TYPES = ["TypeA", "TypeB", "TypeC", "TypeD", "TypeE"]
EVENT_ITERATIONS = 3


class EventGenerator:
    #TODO separate kafka producer and admin clients to own class
    def __init__(self):
        self.kafka_topic_prefix = KAFKA_TOPIC_PREFIX
        self.transaction_key = str(uuid.uuid4())
        self.kafka_producer = None
        self.kafka_admin_client = None
        
    def partition_assignment(
            self, 
            key: str, 
            all_partitions: list[int], 
            available_partitions: list[int] | None = None
        ) -> int:
        #assigns a partition based upon key
        # param key: partioning key
        # param all_partitions: list of all partitions by partition ID
        # return: one partition from [all_partitions]
        
        #for testing co-partitioning across consumers, randomly
        # assign a partition to a key. This is *not* what should be
        # applied in production
        return random.choice(all_partitions)
        # return int(key.decode('UTF-8'))%len(all_partitions)
    
    def setup_admin_client(self) -> KafkaAdminClient:
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
        

    def topic_creation(
        self, 
        topic_name: str, 
        num_partitions: int = 1
        ):
        #an idempotent function to repartition topics
        # note KafkaAdminClient is unstable and liukely to change
        try:
            self.kafka_admin_client.create_partitions({
                topic_name: NewPartitions(num_partitions)
            })
        except InvalidPartitionsError as e:
            if e.errno == 37: # Topic already has n partition(s).
                # it may be better to use KafkaProducer.partitions_for(topic_name) to test 
                # for mutations to partitions instead of relying on err code 37
                pass
            else:
                raise Exception(e)

    # Function to generate Lorem Ipsum sentences
    def generate_lorem_sentence(self) -> str:
        lorem = TextLorem(srange=(5, 10))
        return lorem.sentence()
    
    def setup_producer(self, kafa_config: dict[str, any] = []) -> KafkaProducer :
        kafka_config = kafa_config if kafa_config else {
            'bootstrap_servers': KAFKA_SERVER,
            'partitioner': self.partition_assignment,
            'value_serializer': KAFKA_VALUE_SERIALIZER,
            'key_serializer': KAFKA_VALUE_SERIALIZER,
            'client_id': KAFKA_CLIENT_ID
        }
        self.kafka_producer = KafkaProducer(**kafka_config)

    async def produce_event(self, kafka_key:str , kafka_topic:str ):
        try:
            while True:
                # Produce an event to the Kafka topic
                future = self.kafka_producer.send(
                    self.kafka_topic_prefix + kafka_topic,
                    value={
                        'log': self.generate_lorem_sentence(),
                        'transaction': kafka_key
                        },
                    key=kafka_key
                )
                # synchronous block
                record_metadata = future.get(timeout=10)

                print(f"Event with key {kafka_key} produced for topic {record_metadata.topic}!")
                await asyncio.sleep(2)
                break
                
        except KafkaError as e:
            print(f"Error producing event to Kafka: {e}")
    
    async def initialize_topics(self):
        try:
            self.setup_admin_client()
            for data_type_name in EVENT_DATA_TYPES:
                topic_name = self.kafka_topic_prefix + data_type_name
                self.topic_creation(topic_name, KAFKA_PARTITIONS)
        except Exception as e:
            print(f"Error in initializing topics: {e}")
        finally:
            #close the Kafka admin client
            if self.kafka_admin_client:
                self.kafka_admin_client.close()
    
    async def main(self):
        try:
            await self.initialize_topics() #TODO this should only be invoked once, not per instantiated class
            self.setup_producer()

            # Start the event production coroutine
            await self.produce_event()
        except KeyboardInterrupt:
            # Handle interruption using Ctrl+C
            pass
        finally:
            # Close the Kafka producer
            if self.kafka_producer:
                self.kafka_producer.flush(timeout=5)  # Ensure any buffered messages are delivered
                self.kafka_producer.close()

class EventGeneratorOutOfOrderDataTypes(EventGenerator):
    def __init__(self, *args, **Kwargs):
        super().__init__( *args, **Kwargs)
        self.transaction_key = str(uuid.uuid4())+"(out-of-order)" # testing
    async def produce_event(self):
        for type in random.sample(EVENT_DATA_TYPES, len(EVENT_DATA_TYPES)):
            await super().produce_event(kafka_key=self.transaction_key, kafka_topic=type)
            await asyncio.sleep(random.randrange(10))

class EventGeneratorOmitRandomDataTypes(EventGenerator):
    def __init__(self, *args, **Kwargs):
        super().__init__( *args, **Kwargs)
        self.transaction_key = str(uuid.uuid4())+"(omitted)" # testing
        
    async def produce_event(self, num_to_omit:int = 1):
        #produces events for all but num_to_omit data types
        omit_data_types = random.choices(EVENT_DATA_TYPES, k=num_to_omit)
        
        for type in EVENT_DATA_TYPES:
            if type not in omit_data_types:
                await super().produce_event(kafka_key=self.transaction_key, kafka_topic=type)
                await asyncio.sleep(random.randrange(10))
        return
    
class EventGeneratorAllDataTypes(EventGenerator):        
    async def produce_event(self):
        for type in EVENT_DATA_TYPES:
            await super().produce_event(kafka_key=self.transaction_key, kafka_topic=type)
            await asyncio.sleep(random.randrange(10))
        return

async def run_all():
    out_of_order_events = EventGeneratorOutOfOrderDataTypes()
    events_with_omitted_types = EventGeneratorOmitRandomDataTypes()
    events_with_all_data_types = EventGeneratorAllDataTypes()

    await asyncio.gather(
        out_of_order_events.main(),
        events_with_omitted_types.main(),
        events_with_all_data_types.main()
    )
 

# Run the asyncio event loop
if __name__ == "__main__":
    for i in range(EVENT_ITERATIONS):
        asyncio.run(run_all())
  



