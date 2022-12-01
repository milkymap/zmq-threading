import zmq 
import pickle 
import threading

from uuid import uuid4
from time import perf_counter, sleep
from math import ceil 

from log import logger 
from typing import List, Dict, Tuple, Any  
from dataschema.task_schema import TaskStatus, ListOfTasks, GenericTask, TaskResponse, TaskResponseData

from os import path
from glob import glob 

class ZMQClient:
    def __init__(self, client_broker_address:str, map_topic2nb_switchs:Dict[str, int], nb_threads:int=16):
        self.tasks_mutex = threading.Lock()
        self.tasks_states:Dict[str, Dict[str, TaskStatus]] = {}
        self.tasks_responses:Dict[str, Dict[str, List[Dict[str, Any]] ]] = {}        
        self.client_broker_address = client_broker_address
        self.shutdown_signal = threading.Event()
        self.nb_threads = nb_threads
        self.map_topic2nb_switchs = map_topic2nb_switchs


    def batch_submit_tasks(self, thread_id:int, tasks:ListOfTasks) -> int:
        
        try:
            dealer_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            dealer_socket.connect(self.client_broker_address)
        except Exception:
            return -1 
        
            
        nb_running_tasks = 0
            
        task_cursor = 0
        nb_reps = 0
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 

            if task_cursor >= len(tasks) and nb_running_tasks == 0:
                keep_loop = False 
            
            if task_cursor < len(tasks):
                task_id = str(uuid4())
                priority, topics, task_content = tasks[task_cursor]
                task2send = GenericTask(
                    task_id=task_id,
                    priority=priority, 
                    topics=topics, 
                    task_content=task_content
                ) 

                for topic in topics:
                    nb_subscribers = self.map_topic2nb_switchs.get(topic, 0)
                    nb_running_tasks += nb_subscribers
                    
                self.tasks_states[task_id] = {}
                self.tasks_responses[task_id] = {}

                dealer_socket.send_string('', flags=zmq.SNDMORE)
                dealer_socket.send_pyobj(task2send)

                logger.debug(f'client has sent task {task_id} {nb_running_tasks:03d}')
                task_cursor += 1    
                
            polled_event = dealer_socket.poll(timeout=100)
            if polled_event == zmq.POLLIN: 
                _, broker_encoded_message = dealer_socket.recv_multipart()
                broker_plain_message:TaskResponse = pickle.loads(broker_encoded_message)
                task_status = broker_plain_message.response_type
                task_response_data = broker_plain_message.response_content
                with self.tasks_mutex:
                    self.tasks_states[task_response_data.task_id][task_response_data.topic] = task_status
                    if task_status == TaskStatus.FAILED or task_status == TaskStatus.DONE:
                        if task_response_data.topic not in self.tasks_responses[task_response_data.task_id]:
                            self.tasks_responses[task_response_data.task_id][task_response_data.topic] = []
                        if task_response_data.data is not None: 
                            self.tasks_responses[task_response_data.task_id][task_response_data.topic].append(task_response_data.data)
                        logger.debug(f'client {thread_id:03d} has got a response from broker: rmd:{nb_running_tasks:03d} | rep:{nb_reps} | tot:{len(tasks)}')
                        if task_status == TaskStatus.DONE:
                            nb_running_tasks -= 1
                            nb_reps += 1

        # end while loop 
        logger.debug(f'client {thread_id:03d} has exited its loop')
        dealer_socket.close()

        return 1 
    # end submit_tasks methods

    def submit_tasks(self, list_of_tasks:ListOfTasks):
        try:
            nb_tasks = len(list_of_tasks)
            nb_tasks_per_threads = ceil(nb_tasks / self.nb_threads)

            threads_acc:List[threading.Thread] = []
            thread_id = 0
            for cursor in range(0, nb_tasks, nb_tasks_per_threads):
                partition_of_tasks = list_of_tasks[cursor:cursor+nb_tasks_per_threads]
                threads_acc.append(
                    threading.Thread(
                    target=self.batch_submit_tasks, 
                    args=[thread_id, partition_of_tasks]
                    )
                )
                thread_id = thread_id + 1

            for thread_ in threads_acc:
                thread_.start()


            for thread_ in threads_acc:
                thread_.join()
        except KeyboardInterrupt:
            self.shutdown_signal.set()
        except Exception as e:
            logger.error(e)
        sleep(0.5)
        print(self.tasks_responses)
    
    def __enter__(self):
        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)
        logger.debug('client has initialized zeromq ressources')
        return self 
    
    def __exit__(self, exc_type, exc_value, trackback):
        logger.debug('client will quit the loop')
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
    map_topic2nb_switchs = {
        'PNG': 2, 'JPG': 2, 'JPEG':2
    }

    client = ZMQClient(
        map_topic2nb_switchs=map_topic2nb_switchs,
        client_broker_address='ipc://client_broker.ipc', 
        nb_threads=32
    )

    start = perf_counter()
    with client as clt:
        clt.submit_tasks(list_of_tasks)
    end = perf_counter()
    
    duration = end - start
    logger.debug(f'duration : {duration} seconds | nb tasks : {len(list_of_tasks)}')

