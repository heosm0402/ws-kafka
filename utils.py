def delivery_report(error, msg):
    if error is not None:
        print(f"Delivery failed {msg.key()}: {error}")
        return
    print(f"Delivery succeeded: {msg.key()}, {msg.topic()}, {msg.partition()}, {msg.offset()}")

