import argparse

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import CONST
from model.User import dict_to_user


def main(args):
    topic = CONST.TOPIC_NAME

    sr_conf = {"url": CONST.SCHEMA_REGISTRY}
    sr_client = SchemaRegistryClient(sr_conf)
    schema = sr_client.get_latest_version("avro-test-topic-user-value")

    avro_deserializer = AvroDeserializer(
        sr_client,
        schema.schema,
        dict_to_user
    )

    consumer_conf = {
        "bootstrap.servers": CONST.BOOTSTRAP_SERVER,
        "group.id": args.group,
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while 1:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if user is not None:
                print(f""
                      f"User record key: {msg.key()} | "
                      f"username: {user.username} | "
                      f"birth_year: {user.birth_year} | "
                      f"gender: {user.gender}"
                      )
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer Consumer")
    parser.add_argument("-g", dest="group", default="default_group", help="Consumer group")
    main(parser.parse_args())
