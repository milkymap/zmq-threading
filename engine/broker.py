import zmq 
import pickle
import queue 
import threading

from time import sleep 
from typing import Dict, List, Dict, Tuple, Optional

from log import logger 

from dataschema.task_schema import GenericTask, TaskResponse, Priority
from dataschema.worker_schema import WorkerResponse, WorkerStatus

class ZMQBroker:
    """broker base class"""
    def __init__(self, client_puller_address:str, pusher_worker_address:str):
        self.client_puller_address = client_puller_address
        self.pusher_worker_address = pusher_worker_address  # pusher_source_address 

        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)
    
        self.map_task_id2client_address:Dict[str, bytes] = {}

    def running(self):
        
        poller = zmq.Poller()
        poller.register(self.client_puller_socket, zmq.POLLIN)
        poller.register(self.pusher_worker_socket, zmq.POLLIN)

        available_workers:List[bytes] = []

        keep_loop = True 
        while keep_loop:
            try:
                map_socket2event = dict(poller.poll(timeout=100))
                if len(available_workers) > 0:
                    puller_event = map_socket2event.get(self.client_puller_socket, None)
                    if puller_event is not None: 
                        client_address, _, client_encoded_message = self.client_puller_socket.recv_multipart()
                        client_plain_message:GenericTask = pickle.loads(client_encoded_message)
                        self.map_task_id2client_address[client_plain_message.task_id] = client_address 
                        target_worker_address = available_workers.pop()
                        self.pusher_worker_socket.send_multipart([
                            target_worker_address, 
                            b'', 
                            client_encoded_message
                        ])
                
                pusher_event  = map_socket2event.get(self.pusher_worker_socket, None)
                if pusher_event is not None: 
                    worker_address, _, worker_encoded_message = self.pusher_worker_socket.recv_multipart()
                    worker_plain_message:WorkerResponse = pickle.loads(worker_encoded_message)

                    if worker_plain_message.response_type == WorkerStatus.FREE:
                        available_workers.append(worker_address)

                    if worker_plain_message.response_type == WorkerStatus.RESP:
                        task_response = worker_plain_message.response_content
                        assert task_response is not None 
                        incoming_task_id = task_response.response_content.task_id
                        target_client = self.map_task_id2client_address.get(incoming_task_id, None)
                        if target_client is not None:
                            self.client_puller_socket.send_multipart([target_client, b''], flags=zmq.SNDMORE)
                            self.client_puller_socket.send_pyobj(task_response)
                        else:
                            raise ValueError(f'{incoming_task_id} was not found ...!')
            except KeyboardInterrupt:
                keep_loop = False 
            except Exception as e:
                logger.error(e)
                keep_loop = False
        # end while loop ...! 
        
        self.client_puller_socket.close()
        self.pusher_worker_socket.close()
    def __enter__(self):
        try:
            self.client_puller_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            self.client_puller_socket.bind(self.client_puller_address)
            logger.debug('puller has initialized its zeromq ressources')
        except Exception as e:
            logger.error(e)
            raise Exception('impossible to create client_puller socket')
        
        try:
            self.pusher_worker_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            self.pusher_worker_socket.bind(self.pusher_worker_address)
            logger.debug('pusher has initialized its zeromq ressources')
        except Exception as e:
            self.client_puller_socket.close()
            logger.error(e)
            raise Exception('impossible to create pusher_worker socket')
        return self     
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.ctx.term()
        logger.success('broker has released all zeromq ressources')

if __name__ == '__main__':
    broker_runner = ZMQBroker(
        'tcp://*:1200',
        'ipc://pusher_worker.ipc'
    )
    print(broker_runner)
    with broker_runner as brn:
        brn.running()

