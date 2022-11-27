import zmq 
import pickle 
import threading

from uuid import uuid4

from log import logger 
from typing import List, Dict, Tuple, Any  
from dataschema.task_schema import TaskStatus, ListOfTasks, GenericTask, TaskResponse, TaskResponseData

from os import path
from glob import glob 

class ZMQClient:
    def __init__(self, client_broker_address:str):
        self.tasks_mutex = threading.Lock()
        self.tasks_states:Dict[str, Dict[str, TaskStatus]] = {}
        self.tasks_responses:Dict[str, Dict[str, Any]] = {}
        
        self.client_broker_address = client_broker_address

        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)

    def submit_tasks(self, tasks:ListOfTasks) -> int:
        if len(tasks) < 0:
            return 0
        try:
            nb_running_tasks = 0
            for task in tasks:
                task_id = str(uuid4())
                print(task, task_id)
                priority, topics, task_content = task 
                task2send = GenericTask(
                    task_id=task_id,
                    priority=priority, 
                    topics=topics, 
                    task_content=task_content
                ) 

                nb_running_tasks += len(topics)

                self.tasks_states[task_id] = {}
                self.tasks_responses[task_id] = {}

                self.client_broker_socket.send_string('', flags=zmq.SNDMORE)
                self.client_broker_socket.send_pyobj(task2send)

                logger.debug('client has sent task {task_id}')
                

            keep_loop = True 
            while keep_loop:
                logger.debug(f'nb running tasks {nb_running_tasks}')
                if nb_running_tasks == 0:
                    keep_loop = False 
                polled_event = self.client_broker_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    _, broker_encoded_message = self.client_broker_socket.recv_multipart()
                    broker_plain_message:TaskResponse = pickle.loads(broker_encoded_message)
                    task_status = broker_plain_message.response_type
                    task_response_data = broker_plain_message.response_content
                    self.tasks_states[task_response_data.task_id][task_response_data.topic] = task_status
                    if task_status == TaskStatus.FAILED or task_status == TaskStatus.DONE:
                        nb_running_tasks -= 1
                        self.tasks_responses[task_response_data.task_id][task_response_data.topic] = task_response_data.data
            # end while loop 
            
            print(self.tasks_responses)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(e)
        return 1 
    # end submit_tasks methods
    
    def __enter__(self):
        self.client_broker_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
        self.client_broker_socket.connect(self.client_broker_address)
        logger.debug('client has initialized zeromq ressources')
        return self 
    
    def __exit__(self, exc_type, exc_value, trackback):
        logger.debug('client will quit the loop')
        self.client_broker_socket.close()
        self.ctx.term()
        logger.debug('client has removed zeromq ressources')
                

if __name__ == '__main__':
    path2source_dir = '/home/ibrahima/Pictures'
    image_paths = sorted(glob(path.join(path2source_dir, '*')))
    extensions = []
    priorities = []
    for path2image in image_paths:
        priorities.append(0)
        _, filename = path.split(path2image)
        extension:str = filename.split('.')[-1]
        extensions.append([extension.upper()])

    list_of_tasks = list(zip(priorities, extensions, image_paths))
    
    client = ZMQClient(
        client_broker_address='ipc://client_broker.ipc'
    )

    with client as clt:
        clt.submit_tasks(list_of_tasks)
