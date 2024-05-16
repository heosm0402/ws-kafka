import argparse

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

import CONST
from model.User import dict_to_user


def main(args):
    topic = CONST.TOPIC_NAME
    if args.specific == "True":
        schema = "user_specific.avsc"
    else:
        schema = "user_generic.avsc"

    with open(f"{CONST.PROJECT_ROOT_DIR}/schema/avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {"url": CONST.SCHEMA_REGISTRY}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
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
    parser.add_argument("-p", dest="specific", default="True", help="Avro specific record")
    parser.add_argument("-g", dest="group", default="default_group", help="Consumer group")
    main(parser.parse_args())
