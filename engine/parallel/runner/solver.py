import zmq 
import queue 
import pickle 
import multiprocessing as mp 

from multiprocessing.synchronize import Barrier

import operator as op

from time import sleep

from log import logger 
from typing import List, Tuple, Dict, Optional, Any, Sequence

from engine import ABCSolver
from dataschema.task_schema import Topic, TaskStatus, GenericTask, SpecializedTask
from dataschema.worker_schema import WorkerConfig, WorkerResponse, WorkerStatus

class PRLRNRSolver:
    "parallel worker for runner mode"
    def __init__(self, switch_id:int, solver_id:int, solver_strategy:ABCSolver, worker_barrier:Barrier, worker_responses_queue:mp.Queue, swith_solver_address:str):        
        self.solver_id = solver_id 
        self.switch_id = switch_id 
        self.solver_strategy = solver_strategy 
        self.worker_barrier:Barrier = worker_barrier
        self.worker_responses_queue:mp.Queue[Dict[str, Dict[Topic, Any]]] = worker_responses_queue

        self.tasks_states:Dict[str, Dict[Topic, TaskStatus]] = {}
        self.tasks_responses:Dict[str, Dict[Topic, Any]] = {}

        self.switch_solver_address = swith_solver_address
    
    def running(self) -> int:
        try:
            self.switch_solver_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            self.switch_solver_socket.connect(self.switch_solver_address)
        except Exception as e:
            logger.error(e)
            return -1 
        
        keep_loop = True 
        has_asked_a_task = False 
        while keep_loop:
            try:
                if not has_asked_a_task:
                    self.switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                    self.switch_solver_socket.send_pyobj(
                        WorkerResponse(
                            response_type=WorkerStatus.FREE,
                            response_content=None
                        )
                    )
                    has_asked_a_task = True  # do not ask a task twice 
                
                polled_event = self.switch_solver_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    _, switch_encoded_message = self.switch_solver_socket.recv_multipart()
                    switch_plain_message:SpecializedTask = pickle.loads(switch_encoded_message)
                    self.tasks_states[switch_plain_message.task_id][switch_plain_message.topic] = TaskStatus.RUNNING
                    
                    try:
                        solver_response = self.solver_strategy(switch_plain_message)
                        task_status = TaskStatus.DONE 
                    except Exception as e:
                        task_status = TaskStatus.FAILED
                        error_message = f'{e}'
                        logger.error(error_message)
                        solver_response = error_message
                    
                    self.tasks_states[switch_plain_message.task_id][switch_plain_message.topic] = task_status 
                    self.tasks_responses[switch_plain_message.task_id][switch_plain_message.topic] = solver_response

                    has_asked_a_task = False
            except KeyboardInterrupt:
                keep_loop = False 
            except Exception as e:
                logger.error(e)
                keep_loop = False  
        # end while loop 
        
        
    def __enter__(self):
        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)
        try:
            self.solver_strategy.initialize()  # heavy intialization such as load a deep learning model 
            self.worker_barrier.wait(timeout=30)
        except Exception as e:
            logger.error(e)
            exit(1)  # end the code 
        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass 
    
        

