# message_system

from broker import IMessageBroker, KafkaBroker


class MessagingSystem:

    def __init__(self, message_broker: IMessageBroker) -> None:
        self.mb = message_broker

    def readMessages(self, data: dict) -> bool:
        received = self.mb.readMessages(data)
        if received:
            return True
        else:
            return False

    def sendMessages(self, data: dict) -> bool:
        sent = self.mb.sendMessages(data)
        if sent:
            return True
        else:
            return False


data = {
    'command': 'send',
    'channel': 'chat',
    'server': 'localhost:9092',
    'group': "hello",
    'from': 'start',
    "message": "Welcome to Uganda"
}

data2 = {
    'command': 'send',
    'channel': 'chat',
    'server': 'localhost:9092',
    'group': "hello",
    'from': 'start'
}

# ms = MessagingSystem(KafkaBroker())
# ms.sendMessages(data)
# ms.readMessages(data2)
