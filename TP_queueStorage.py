from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

import os, uuid
import queue
import time
import enum

#key1: tpqueuestorage (nom de mon compte de stockage)
# clé : 3hHys6TwNayFo9XzSZxvNv+VfOHJy9rTtm+BunehhVdf17g3FxYnGL5JjBcxA8eLCbtxFMc3f9qh+AStrWj88Q==
# chaine de connexion : DefaultEndpointsProtocol=https;AccountName=tpqueuestorage;AccountKey=3hHys6TwNayFo9XzSZxvNv+VfOHJy9rTtm+BunehhVdf17g3FxYnGL5JjBcxA8eLCbtxFMc3f9qh+AStrWj88Q==;EndpointSuffix=core.windows.net

# Retrieve the connection string from an environment
# variable named AZURE_STORAGE_CONNECTION_STRING
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Create a unique name for the queue
q_name = "TP_queueStorage" + str(uuid.uuid4())

# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Creating queue: " + q_name)
queue_client = QueueClient.from_connection_string(connect_str, q_name)

# Create the queue
queue_client.create_queue()

# Setup Base64 encoding and decoding functions
base64_queue_client = QueueClient.from_connection_string(
                            conn_str=connect_str, queue_name=q_name,
                            message_encode_policy = BinaryBase64EncodePolicy(),
                            message_decode_policy = BinaryBase64DecodePolicy()
                        )

# un processus prodcuteur qui ajoute des messages à intervalle régulier 
# inserer un message
messages = u"coucou"
print("message: " + messages)
queue_client.send_message(messages,  time_to_live =5*60)

# deux processus consommateurs qui récupèrent et traitent ces messages à intervalle régulier
# = recuperer des messages 
# obtenir les messages + traite chaque msg et definit le delai d'invisibilité 5min
messages = queue_client.receive_messages(messages_per_page=5, visibility_timeout=5*60)

for msg_batch in messages.by_page():
   for msg in msg_batch:
      print("Batch dequeue message: " + msg.content)
      queue_client.delete_message(msg)

# 1 fois sur 10 le processus doit echouer 
if (queue_client.receive_message > 10):
    queue_client.send_message("le processus a echouer")

#modifier le contenu d'un message en file d'attente
messages = queue_client.receive_messages()
list_result = next(messages)

message = queue_client.update_message(
        list_result.id, list_result.pop_receipt,
        visibility_timeout=0, content=u'Hello World Again')

print("Updated message to: " + message.content)