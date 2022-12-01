import zmq 
import pickle 

import multiprocessing as mp 
from multiprocessing.synchronize import Barrier, Event

from log import logger 
from time import sleep 

from engine import ABCSolver
from dataschema.task_schema import Topic, TaskStatus, GenericTask, SpecializedTask, TaskResponseData, TaskResponse
from dataschema.worker_schema import WorkerConfig, WorkerResponse, WorkerStatus

class PRLRNRSolver:
    "parallel worker for runner mode"
    def __init__(self, switch_liveness:Event, solver_start_loop:Event, shutdown_signal:Event, solver2switch_queue:mp.Queue, service_name:str, switch_id:int, solver_id:int, solver_strategy:ABCSolver, solver_barrier:Barrier, swith_solver_address:str):        
        self.solver_id = solver_id 
        self.switch_id = switch_id 
        self.service_name = service_name

        self.solver_strategy = solver_strategy 
        self.switch_solver_address = swith_solver_address.replace(
            '.ipc', 
            f'_{switch_id}.ipc'
        )  # router server 

        self.solver_start_loop:Event = solver_start_loop
        self.solver_barrier:Barrier = solver_barrier
        self.switch_liveness:Event = switch_liveness
        self.shutdown_signal:Event = shutdown_signal

        self.solver2switch_queue:mp.Queue[bytes] = solver2switch_queue

        self.ctx = zmq.Context()
        self.ctx.setsockopt(zmq.LINGER, 0)

        self.is_ready = 0
        self.zmq_initialized = 0 
    
    def running(self) -> int:
        if not self.is_ready:
            return 1 

        self.switch_solver_socket.send_string('', flags=zmq.SNDMORE)
        self.switch_solver_socket.send_pyobj(
            WorkerResponse(
                response_type=WorkerStatus.JOIN,
                response_content=None 
            )
        )
        
        logger.debug(f'solver {self.solver_id:03d} has connected to the switch ({self.service_name})')
        returned_value = self.solver_start_loop.wait(timeout=10)  # waiting signal from the switch process 
        
        if not returned_value:
            logger.debug(f'solver {self.solver_id:03d} wait too long to get the signal from the switch {self.service_name}')
            if not self.shutdown_signal.is_set():
                self.shutdown_signal.set()
            return 1 

        nb_hits = 0
        keep_loop = True 
        has_asked_a_task = False 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
           
            try:
                if not has_asked_a_task:
                    self.solver2switch_queue.put(f'{self.solver_id:03d}'.encode())
                    has_asked_a_task = True  # do not ask a task twice 
                
                polled_event = self.switch_solver_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    _, switch_encoded_message = self.switch_solver_socket.recv_multipart()
                    switch_plain_message:SpecializedTask = pickle.loads(switch_encoded_message)
                    self.switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                    self.switch_solver_socket.send_pyobj(
                        WorkerResponse(
                            response_type=WorkerStatus.RESP,
                            response_content=TaskResponse(
                                response_type=TaskStatus.RUNNING,
                                response_content=TaskResponseData(
                                    topic=switch_plain_message.topic,
                                    task_id=switch_plain_message.task_id, 
                                    data=None 
                                )
                            )
                        )
                    )

                    try:
                        solver_response = self.solver_strategy(switch_plain_message)
                        task_status = TaskStatus.DONE 
                    except Exception as e:
                        task_status = TaskStatus.FAILED
                        error_message = f'{e}'
                        logger.error(error_message)
                        solver_response = error_message
                    # send response 
                    
                    self.switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                    self.switch_solver_socket.send_pyobj(
                        WorkerResponse(
                            response_type=WorkerStatus.RESP,
                            response_content=TaskResponse(
                                response_type=task_status, 
                                response_content=TaskResponseData(
                                    task_id=switch_plain_message.task_id,
                                    topic=switch_plain_message.topic, 
                                    data={
                                        self.service_name:solver_response
                                    }
                                )
                            )
                        )
                    )
                    has_asked_a_task = False
                    nb_hits += 1
            except KeyboardInterrupt:
                keep_loop = False 
            except Exception as e:
                logger.error(e)
                keep_loop = False  
        # end while loop 
        
        logger.debug(f'solver {self.solver_id:03d} got {nb_hits:05d} hits form the switch {self.service_name}')
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()

        return 0
        
    def __enter__(self):
        returned_value = self.switch_liveness.wait(timeout=5)
        if not returned_value:
            logger.debug(f'solver {self.solver_id:03d} wait too long to get the signal from the switch {self.service_name}')
            return self 
        
        try:
            self.switch_solver_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            self.switch_solver_socket.setsockopt_string(zmq.IDENTITY, f'{self.solver_id:03d}')
            self.switch_solver_socket.connect(self.switch_solver_address)
            logger.debug(f'solver {self.solver_id:03d} has initialized its zeromq socket for the switch {self.service_name}')
            self.zmq_initialized = 1 
        except Exception as e:
            logger.error(e)
            return self 
         
        try:
            self.solver_strategy.initialize()  # heavy intialization such as load a deep learning model 
            self.solver_barrier.wait(timeout=30)
        except Exception as e:
            logger.error(e)
            return self 
        
        self.is_ready = 1
        return self 
    
    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug(f'solver {self.solver_id:03d} will quit the switch {self.service_name}')
        if self.zmq_initialized:
            self.switch_solver_socket.close()
            logger.debug(f'solver {self.solver_id:03d} was disconnected from the switch {self.service_name}')

        self.ctx.term()
        logger.debug(f'solver {self.solver_id:03d} has released its zmq context for the switch {self.service_name}')
        return True 
        
    