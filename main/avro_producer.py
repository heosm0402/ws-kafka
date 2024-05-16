import argparse
import os.path
import random
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from model.User import User, user_to_dict
import CONST
import utils


name_list = ["user1", "user2", "user3", "user4", "user5", "user6"]


def main(args):
    global name_list
    topic = CONST.TOPIC_NAME

    sr_conf = {"url": CONST.SCHEMA_REGISTRY}
    sr_client = SchemaRegistryClient(sr_conf)
    schema = sr_client.get_latest_version("avro-test-topic-user-value")

    avro_serializer = AvroSerializer(
        sr_client,
        schema.schema,
        user_to_dict
    )

    string_serializer = StringSerializer("UTF-8")
    conf = {"bootstrap.servers": CONST.BOOTSTRAP_SERVER}
    producer = Producer(conf)

    while 1:
        user = User(
            username=name_list[random.randint(0, len(name_list) - 1)],
            birth_year=random.randint(1994, 2010),
            gender=random.randint(0, 1),
        )
        producer.produce(
            topic,
            key=string_serializer(str(uuid4())),
            value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
            on_delivery=utils.delivery_report
        )

        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer Producer")
    main(parser.parse_args())
