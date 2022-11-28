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
    
        self.puller2pusher_priority_queue:queue.PriorityQueue[Tuple[Priority, bytes]] = queue.PriorityQueue()
        self.pusher2puller_simple_queue:queue.SimpleQueue[TaskResponse] = queue.SimpleQueue()
        self.map_task_id2client_address:Dict[str, bytes] = {}

        self.shutdown_signal = threading.Event()
        self.threads_mutex = threading.Lock()
        self.quit_condition = threading.Condition()
        self.nb_threads = 0 
        
    def puller(self) -> Optional[int]:
        try:
            client_puller_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            client_puller_socket.bind(self.client_puller_address)
            logger.debug('puller has initialized its zeromq ressources')
        except Exception as e:
            logger.error(e)
            return -1
        
        with self.threads_mutex:
            self.nb_threads += 1 

        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                logger.debug('puller got the shutdown signal from broker main thread')
                keep_loop = False 
            try:
                message_from_pusher:TaskResponse = self.pusher2puller_simple_queue.get(block=True, timeout=0.01)
                incoming_task_id = message_from_pusher.response_content.task_id
                target_client = self.map_task_id2client_address.get(incoming_task_id, None)
                if target_client is not None:
                    client_puller_socket.send_multipart([target_client, b''], flags=zmq.SNDMORE)
                    client_puller_socket.send_pyobj(message_from_pusher)
                else:
                    raise ValueError(f'{incoming_task_id} was not found ...!')
            except queue.Empty:
                pass 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 

            try:
                polled_event = client_puller_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    client_address, _, client_encoded_message = client_puller_socket.recv_multipart()
                    client_plain_message:GenericTask = pickle.loads(client_encoded_message)
                    self.puller2pusher_priority_queue.put(
                        (client_plain_message.priority, client_encoded_message)
                    )
                    self.map_task_id2client_address[client_plain_message.task_id] = client_address 
                # end if event polling 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False
        # end while loop 
        
        with self.quit_condition:
            with self.threads_mutex:
                client_puller_socket.close()
                self.nb_threads -= 1
                logger.debug('puller has released all zeromq ressources')

            self.quit_condition.notify()  # wake up the main thread 
        # free the underlying lock 
        return None 
    # end function puller 
    
    def pusher(self) -> Optional[int]:
        try:
            pusher_worker_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            pusher_worker_socket.bind(self.pusher_worker_address)
            logger.debug('pusher has initialized its zeromq ressources')
        except Exception as e:
            logger.error(e)
            return -1 
        
        available_workers:queue.SimpleQueue[bytes] = queue.SimpleQueue()

        with self.threads_mutex:
            self.nb_threads += 1 
        
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                logger.debug('pusher got the shutdown signal from broker main thread')
                keep_loop = False 

            try:
                polled_event = pusher_worker_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    worker_address, _, worker_encoded_message = pusher_worker_socket.recv_multipart()
                    worker_plain_message:WorkerResponse = pickle.loads(worker_encoded_message)

                    if worker_plain_message.response_type == WorkerStatus.FREE:
                        available_workers.put(worker_address)
                    if worker_plain_message.response_type == WorkerStatus.RESP:
                        task_response = worker_plain_message.response_content
                        assert task_response is not None 
                        self.pusher2puller_simple_queue.put(task_response)
                # end if event polling 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
            
            try:
                worker_address = available_workers.get(block=True, timeout=0.01)
                try:
                    _, client_encoded_message = self.puller2pusher_priority_queue.get(block=True, timeout=0.01)  # ignore the priority
                    pusher_worker_socket.send_multipart(
                        [worker_address, b'', client_encoded_message
                    ])
                except queue.Empty:
                    available_workers.put(worker_address)
            except queue.Empty:
                pass 
        # end while loop 

        with self.quit_condition:
            with self.threads_mutex:
                pusher_worker_socket.close()
                self.nb_threads -= 1
                logger.debug('pusher has released all zeromq ressources')
            self.quit_condition.notify()  # wake up the main thread

        # free the underlying lock 
        return None 
    # end function pusher 

    def running(self):
        try:
            self.puller_thread.start()
            self.pusher_thread.start()
        
            self.puller_thread.join()
            self.pusher_thread.join()
        except KeyboardInterrupt:
            logger.warning('ctr+c was catched by broker main thread')  # notify puller and pusher thread to quit their loops  
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
        self.quit_condition.acquire()
        logger.debug('broker quit the context manager and is waiting 5s to end all ressources')
        self.shutdown_signal.set()
        returned_value = self.quit_condition.wait_for(
            predicate=lambda: self.nb_threads == 0, 
            timeout=5
        )
        if not returned_value:
            logger.warning('broker main thread has waited too long, it will force to quit')
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

