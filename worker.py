import zmq 
import queue 
import pickle 
import threading

import uuid 
import numpy as np 
from time import sleep

from log import logger 
from typing import (
    List, Tuple, Dict, 
    Optional, Any, Callable
)

from dataschema import Priority, TaskStatus, SolverStatus, Task
from solver import ZMQSolver 

class ZMQWorker:

    def __init__(self, 
            jobs:List[Tuple[int, List[str], Any]],  # list of (priority, [topics], message) 
            swtich_config:List[Tuple[List[str], ZMQSolver]],   
            nb_solvers_per_switch:int,
            source2switch_address:str, 
            switch_solver_address:str, 
        ):

        # add instance checking for switch_config 

        nb_switchs = len(swtich_config)
        
        assert len(jobs) > 0
        assert nb_switchs > 0 
        assert nb_solvers_per_switch > 0 

        self.ctx = zmq.Context()
        
        self.jobs = jobs 
        self.nb_switchs = nb_switchs
        self.swtich_config = swtich_config
        
        self.nb_connected_swtichs = 0
        self.nb_solver_per_switchs = nb_solvers_per_switch
        self.source2switch_address = source2switch_address
        self.switch_solver_address = switch_solver_address

        self.ctx.setsockopt(zmq.LINGER, 0)  # global option for all sockets 

        self.tasks_mutex = threading.Lock()
        self.tasks_states:Dict[str, Dict[str, TaskStatus]] = {}
        self.tasks_responses:Dict[str, Dict[str, Any]] = {}
        self.nb_running_tasks = 0

        self.shutdown_signal = threading.Event()
        self.source_quitloop = threading.Event()
        self.source_liveness = threading.Event()

        self.source_switch_condition = threading.Condition()
        
        self.switchs_barrier = threading.Barrier(parties=self.nb_switchs) 
        self.switchs_liveness:List[threading.Event] = [] 
        self.switchs_quitloop:List[threading.Event] = []
        self.switchs_nb_connected_solvers:List[int] = []

        self.switch_solver_conditions:List[threading.Condition] = []
        
        for _ in range(self.nb_switchs):
            self.switchs_quitloop.append(threading.Event())
            self.switchs_liveness.append(threading.Event())
            self.switch_solver_conditions.append(
                threading.Condition()
            )
            self.switchs_nb_connected_solvers.append(0)
        
        self.map_topic2nb_switchs = {}

    def source(self) -> None:
        try:
            source2switch_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            source2switch_socket.bind(self.source2switch_address)
        except Exception as e:
            logger.error(e) 
        
        logger.success('source has initialized its zeromq ressources')
        self.source_switch_condition.acquire()
        logger.success('source is waiting for switchs to connect')
        self.source_liveness.set()  # notify all switch that the source is up 
        returned_value = self.source_switch_condition.wait_for(
            predicate=lambda: self.nb_connected_swtichs == self.nb_switchs, 
            timeout=10
        )

        if not returned_value:
            logger.warning('source wait too long for switchs to connect')
            source2switch_socket.close()
            return -1 
        
        logger.debug('all switchs are connected to the source')
        job_cursor = 0
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            
            if job_cursor >= len(self.jobs):
                # all jobs were sent 
                with self.tasks_mutex:
                    if self.nb_running_tasks == 0:
                        keep_loop = False 
                        logger.debug('all tasks were done : system will shutdown')
                        print(self.tasks_responses)
                    else:
                        logger.debug(f'nb running tasks(topic) : {self.nb_running_tasks}')
                # end mutex context manager : free the lock 
                
            try:  # add a rate limiter based on the rate of response from switch thread 
                if job_cursor < len(self.jobs) and len(self.map_topic2nb_switchs) > 0:
                    with self.tasks_mutex:
                        current_job:Tuple[int, List[str], str] = self.jobs[job_cursor]
                        priority, topics, current_message = current_job  # ignore priority
                        task_id = str(uuid.uuid4())  # unique task id 
                        self.tasks_responses[task_id] = {} 
                        self.tasks_states[task_id] = {}
                            
                        for current_topic in topics:        
                            if current_topic in self.map_topic2nb_switchs:
                                if self.map_topic2nb_switchs[current_topic] > 0:
                                    current_task = Task(
                                        task_id=task_id, 
                                        topic=current_topic, 
                                        content=current_message
                                    )
                                    print(current_task)

                                    source2switch_socket.send_string(current_topic, flags=zmq.SNDMORE)
                                    source2switch_socket.send_pyobj(current_task)
                                    self.tasks_states[task_id][current_topic] = TaskStatus.PENDING
                            
                                    self.nb_running_tasks += 1

                                else:
                                    self.tasks_states[task_id][current_topic] = TaskStatus.FAILED
                            else:
                                logger.debug(f'{current_topic} has no target subcribers | job {current_message} was not processed')
                                self.tasks_states[task_id][current_topic] = TaskStatus.FAILED
                        # end for loop over topics  
                        job_cursor = job_cursor + 1
                    # end mutext context manager : free the lock 
                # end if available workers 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
        self.source_switch_condition.acquire()
        self.source_quitloop.set()  # notify all switch to disconnect their sockets 
        self.source_switch_condition.wait_for(
            predicate=lambda: self.nb_connected_swtichs == 0
        )
        logger.debug('all switch are disconnected from the source')
        source2switch_socket.close()
        logger.success('source has released its zeromq ressources')
        self.threads_flag.set()  # notify main thread to end the zeromq context 
    # end function source 
        
    def switch(self, switch_id:int) -> None:
        liveness = self.source_liveness.wait(timeout=5)
        if not liveness:
            logger.debug(f'switch {switch_id:03d} wait too long to get the signal from source')
            return -1 
        
        logger.debug(f'switch {switch_id:03d} has received the signal from source')
        try:
            switch_solver_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
            switch_solver_socket.bind(f'{self.switch_solver_address}_{switch_id:03d}')

            source2switch_socket:zmq.Socket = self.ctx.socket(zmq.SUB)
            source2switch_socket.connect(self.source2switch_address)

            logger.success(f'switch {switch_id:03d} has initialized its zeromq ressource') 
        except Exception as e:
            logger.error(e)
            return -1 
        
        available_solvers = queue.SimpleQueue() 
        
        try:
            self.swtich_config[switch_id][1].initialize()  # 
            self.switchs_barrier.wait(timeout=5)
            logger.debug(f'switch {switch_id:03d} pass the barrier')
            sleep(0.01)  # waiting 100ms => 
        except threading.BrokenBarrierError:
            logger.warning(f'switch {switch_id:03d} wait too long at the barrier')
            switch_solver_socket.close()
            source2switch_socket.close()
            return -1 

        self.switch_solver_conditions[switch_id].acquire()
        logger.debug(f'switch {switch_id:03d} is waiting for solver to connect')
        self.switchs_liveness[switch_id].set()  # notify all solvers that belong to this group(switch_id) to start their loop 
        returned_value = self.switch_solver_conditions[switch_id].wait_for(
            predicate=lambda: self.switchs_nb_connected_solvers[switch_id] == self.nb_solver_per_switchs, 
            timeout=10
        )

        if not returned_value:
            logger.warning(f'solvers take too long time to connect to the switch {switch_id:03d}')
            switch_solver_socket.close()
            source2switch_socket.close()
            return -1 
        
        logger.debug(f'all solvers are connected to the switch {switch_id:03d}')
        
        list_of_topics:List[str] = self.swtich_config[switch_id][0]
        with self.source_switch_condition:
            logger.debug(f'switch {switch_id:03d} got the lock and will authenticate with {list_of_topics}')
            for topic in list_of_topics:
                source2switch_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                if topic not in self.map_topic2nb_switchs:
                    self.map_topic2nb_switchs[topic] = 0
                self.map_topic2nb_switchs[topic] += 1 
            self.nb_connected_swtichs += 1             
            self.source_switch_condition.notify()  # wake up the source
            logger.debug(f'switch {switch_id:03d} is ready to process message')
        # free the unferlying lock so that, other switch can communicate with the source
        
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            try:
                # check if there is an available solver 
                # if yes then, pull data from source and send it to the solver 
                solver_address:bytes = available_solvers.get(block=True,  timeout=0.01)
                polled_event = source2switch_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    message_from_source:Tuple[bytes, bytes]
                    message_from_source = source2switch_socket.recv_multipart()
                    _, source_encoded_message = message_from_source  # ignore the topic 
                    source_plain_message:Task = pickle.loads(source_encoded_message)
                    switch_solver_socket.send_multipart([solver_address, b''], flags=zmq.SNDMORE)
                    switch_solver_socket.send(source_encoded_message)
                    with self.tasks_mutex:
                        self.tasks_states[source_plain_message.task_id][source_plain_message.topic] = TaskStatus.SCHEDULED        
                else:
                    available_solvers.put(solver_address)
            except queue.Empty:
                pass 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
            
            try:
                # data from solver 
                polled_event = switch_solver_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    message_from_solver:Tuple[bytes, bytes, bytes, bytes]
                    message_from_solver = switch_solver_socket.recv_multipart()
                    solver_address, _, solver_encoded_message = message_from_solver
                    if solver_encoded_message == SolverStatus.FREE:
                        available_solvers.put(solver_address)
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop  
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
        self.switch_solver_conditions[switch_id].acquire()
        self.switchs_quitloop[switch_id].set()
        self.switch_solver_conditions[switch_id].wait_for(
            predicate=lambda:self.switchs_nb_connected_solvers[switch_id] == 0
        )
        logger.debug(f'all solvers are disconnected to the switch {switch_id:03d}')
        self.source_quitloop.wait()
        with self.source_switch_condition: 
            source2switch_socket.close()
            switch_solver_socket.close()
            self.nb_connected_swtichs -= 1
            self.source_switch_condition.notify()
            logger.success(f'switch {switch_id:03d} has released its zeromq ressource')
        # free the underlying lock 
    # end function swtich 
    
    def solver(self, switch_id:int, solver_id:int) -> None:
        liveness = self.switchs_liveness[switch_id].wait(timeout=5)
        if not liveness:
            logger.warning(f'solver {solver_id:03d} wait too long to get the signal from {switch_id:03d}')
            return -1 
        
        logger.debug(f'solver {solver_id:03d} {switch_id:03d} has received the signal from the switch')
        try:
            swtich_solver_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            swtich_solver_socket.connect(f'{self.switch_solver_address}_{switch_id:03d}')
        except Exception as e:
            logger.error(e)
            return -1
        
        logger.debug(f'solver {solver_id:03d} {switch_id:03d} has initialized its zeromq ressources')
        with self.switch_solver_conditions[switch_id]:
            self.switchs_nb_connected_solvers[switch_id] += 1 
            self.switch_solver_conditions[switch_id].notify()
            logger.debug(f'solver {solver_id:03d} {switch_id:03d} is ready to process messages')

        is_busy = 0
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            
            if not is_busy:
                swtich_solver_socket.send_multipart([b'', SolverStatus.FREE])
                is_busy = 1
                
            try:
                polled_event = swtich_solver_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    message_from_switch:Tuple[bytes, bytes]
                    message_from_switch = swtich_solver_socket.recv_multipart()
                    _, switch_encoded_message = message_from_switch  # ignore delimiter 
                    switch_plain_message:Task = pickle.loads(switch_encoded_message)
                    with self.tasks_mutex:
                        self.tasks_states[switch_plain_message.task_id][switch_plain_message.topic] = TaskStatus.RUNNING
                    try:
                        response = self.swtich_config[switch_id][1](task=switch_plain_message)
                        with self.tasks_mutex:
                            self.tasks_states[switch_plain_message.task_id][switch_plain_message.topic] = TaskStatus.DONE 
                            self.tasks_responses[switch_plain_message.task_id][switch_plain_message.topic] = response
                            self.nb_running_tasks -= 1 
                    except Exception as e:
                        logger.error(e)
                        with self.tasks_mutex:
                            self.tasks_states[switch_plain_message.task_id][switch_plain_message.topic] = TaskStatus.FAILED
                            self.nb_running_tasks -= 1
                    is_busy = 0  # the solver is free and can ask a new job  
                # end event polling 
            except zmq.ZMQError:
                pass 
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
        self.switchs_quitloop[switch_id].wait()
        with self.switch_solver_conditions[switch_id]:
            swtich_solver_socket.close()
            self.switchs_nb_connected_solvers[switch_id] -= 1 
            self.switch_solver_conditions[switch_id].notify()
            logger.debug(f'solver {solver_id:03d} {switch_id:03d} has released its zeromq ressource')
        # free underlying lock  
    # end function solver 
    
    def running(self):
        # run all threads :  
        try:    
            self.source_thread.start()
            for switch_thread in self.array_of_swtich_thread:
                switch_thread.start()
            for solver_thread in self.array_of_solver_thread:
                solver_thread.start()
            
            # wait for threads to terminate their loops
            solver_thread.join()
            for switch_thread in self.array_of_swtich_thread:
                switch_thread.join()
            for solver_thread in self.array_of_solver_thread:
                solver_thread.join()
            
        except KeyboardInterrupt:
            logger.debug('ctl+c was catched => all threads will quit their loop')
            self.shutdown_signal.set()  # notify all thread to quit their loop 
        except Exception as e:
            logger.error(e)
    # end function running 

    def __enter__(self):
        try:
            # thread initialization 
            self.threads_flag = threading.Event()

            self.source_thread = threading.Thread(target=self.source)
            self.array_of_swtich_thread:List[threading.Thread] = []
            self.array_of_solver_thread:List[threading.Thread] = []
            
            for switch_id in range(self.nb_switchs):
                switch_thread = threading.Thread(target=self.switch, args=[switch_id])
                self.array_of_swtich_thread.append(switch_thread)
                for solver_id in range(self.nb_solver_per_switchs):
                    solver_thread = threading.Thread(target=self.solver, args=[switch_id, solver_id])
                    self.array_of_solver_thread.append(solver_thread)
                # end for loop over solver_ids 
            # end for loop over switch_ids 
                    
        except Exception as e:
            logger.error(e)
        return self 
    # end special method 
        
    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug('main thread is waiting for all threads to quit their loop')
        self.threads_flag.wait(timeout=10)  # wait 10s 
        self.ctx.term()
        logger.success('main thread has released all ressources')
    # end special method 