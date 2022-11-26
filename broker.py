import zmq 
import pickle
import queue 
import threading

import uuid
from time import sleep 
from typing import Dict, List, Tuple 

from log import logger 
from dataschema import Job, WorkerStatus

class ZMQBroker:

    def __init__(self, client_puller_address:str, pusher_worker_address:str):
        self.client_puller_address = client_puller_address
        self.pusher_worker_address = pusher_worker_address

        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)
    
        self.puller2pusher_priority_queue = queue.PriorityQueue()
        self.pusher2puller_simple_queue = queue.SimpleQueue()
        self.map_task_id2worker_address:Dict[str, bytes] = {}

    def puller(self) -> None:
        try:
            client_puller_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            client_puller_socket.bind(self.client_puller_address)
        except Exception as e:
            logger.error(e)
            return -1
        
        keep_loop = True 
        while keep_loop:
            try:
                polled_event = client_puller_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    client_address, _, client_encoded_message = client_puller_socket.recv_multipart()
                    client_plain_message:Job = pickle.loads(client_encoded_message)
                    self.puller2pusher_priority_queue.put(
                        (client_plain_message.priority, client_encoded_message)
                    )
                    self.map_task_id2worker_address[client_plain_message.task_id] = None 
                # end if event polling 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False
        # end while loop 
        
        client_puller_socket.close()
    # end function puller 
    
    def pusher(self) -> None:
        try:
            pusher_worker_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            pusher_worker_socket.bind(self.pusher_worker_address)
        except Exception as e:
            logger.error(e)
            return -1 
        
        available_workers = queue.SimpleQueue()
        
        keep_loop = True 
        while keep_loop:
            try:
                polled_event = pusher_worker_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    worker_address, _, worker_encoded_message = pusher_worker_socket.recv_multipart()
                    if worker_encoded_message == WorkerStatus.FREE:
                        available_workers.put(worker_address)
                # end if event polling 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
            
            try:
                worker_address = available_workers.get(block=True, timeout=0.01)
                try:
                    _, client_encoded_message = self.puller2pusher_priority_queue.get(block=True, timeout=0.01)
                    pusher_worker_socket.send_multipart([worker_address, b'', client_encoded_message])
                except queue.Empty:
                    available_workers.put(worker_address)
            except queue.Empty:
                pass 
        # end while loop 
        
        pusher_worker_socket.close()
    # end function pusher 

    def running(self):
        try:
            self.puller_thread.start()
            self.pusher_thread.start()
        
            self.puller_thread.join()
            self.pusher_thread.join()
        except KeyboardInterrupt:
            pass 
        except Exception as e:
            logger.error(e)

    def __enter__(self):
        try:
            self.puller_thread = threading.Thread(
                target=self.puller,
                args=[]
            )
            self.pusher_thread = threading.Thread(
                target=self.pusher, 
                args=[]
            )
        except Exception as e:
            logger.error(e)

        return self     
    
    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug('broker quit the context manager')
        sleep(2)
        self.ctx.term()
        logger.success('broker has released all zeromq ressources')

