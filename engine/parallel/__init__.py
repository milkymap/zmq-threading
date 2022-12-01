import zmq 
import multiprocessing as mp 

from typing import List, Dict, Any , Optional
from time import perf_counter, sleep 

from dataschema.worker_schema import WorkerConfig, WorkerResponse, WorkerStatus
from dataschema.task_schema import GenericTask, SpecializedTask, TaskStatus

from engine.parallel.runner.switch import PRLRNRSwitch

from log import logger 

class PRLServer:
    def __init__(self):
        pass 

class PRLRunner:
    def __init__(self, list_of_tasks:List[GenericTask], worker_config:WorkerConfig, source2switch_address:str, switch2source_address:str, switch_solver_address:str):
        assert len(list_of_tasks) > 0
        assert len(worker_config.list_of_switch_configs) > 0

        self.ctx = zmq.Context()

        self.ctx.setsockopt(zmq.LINGER, 0)
        self.nb_switchs = len(worker_config.list_of_switch_configs)

        self.worker_config = worker_config 
        self.list_of_tasks = list_of_tasks

        self.source_liveness = mp.Event()
        self.shutdown_signal = mp.Event()
        self.switch_start_loop = mp.Event()

        self.source2switch_address = source2switch_address
        self.switch2source_address = switch2source_address
        self.switch_solver_address = switch_solver_address

        self.source2switch_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
        self.switch2source_socket:zmq.Socket = self.ctx.socket(zmq.PULL)

        self.map_topic2nb_switchs:Optional[Dict[str, int]] = None 
        self.tasks_states:Dict[str, Dict[str, TaskStatus]] = {}
        self.tasks_responses:Dict[str, Dict[str, List[Dict[str, Any]]] ] = {}        
        
        self.is_ready = 0 
        self.zmq_initialized = 0

    def running(self) -> int:
        if not self.is_ready:
            return 1 

        try:
            worker_process = mp.Process(target=self.__start_worker)
            worker_process.start()
        except Exception as e:
            logger.error(e)
            return 1 
        
        self.source_liveness.set()  # notify switch to start its initialization 
        logger.debug('source is waiting to get connection from the switchs')
        polled_event = self.switch2source_socket.poll(timeout=15000)  # wait 15s for incoming data from switch 
        if polled_event == zmq.POLLIN: 
            self.map_topic2nb_switchs = self.switch2source_socket.recv_pyobj()
        
        if self.map_topic2nb_switchs is None: 
            logger.debug('no switch were available, source will quit the loop')
            if not self.shutdown_signal.is_set():
                self.shutdown_signal.set()
            return 1 
        
        self.switch_start_loop.set()  # notify switchs to start their loops 

        nb_running_tasks = 0
        max_nb_running_tasks = self.worker_config.max_nb_running_tasks

        start = perf_counter()
        cursor = 0
        keep_loop = True
        nb_responses = 0 
        while keep_loop:
            try:
                if self.shutdown_signal.is_set():
                    keep_loop = False 
                if cursor >= len(self.list_of_tasks) and nb_running_tasks == 0:
                    keep_loop = False 
                
                polled_event = self.switch2source_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN: 
                    switch_plain_message:WorkerResponse = self.switch2source_socket.recv_pyobj()
                    if switch_plain_message.response_type == WorkerStatus.RESP:
                        task_response = switch_plain_message.response_content
                        if task_response is not None:
                            task_status = task_response.response_type
                            task_response_data = task_response.response_content
                            self.tasks_states[task_response_data.task_id][task_response_data.topic] = task_status
                        
                            if task_status in [TaskStatus.FAILED, TaskStatus.DONE]:
                                if task_response_data.topic not in self.tasks_responses[task_response_data.task_id]:
                                    self.tasks_responses[task_response_data.task_id][task_response_data.topic] = []
                                if task_response_data.data is not None:
                                    self.tasks_responses[task_response_data.task_id][task_response_data.topic].append(task_response_data.data)
                                nb_running_tasks -= 1
                                nb_responses += 1
                                logger.debug(f'source has got a response from broker: rmd:{nb_running_tasks:05d} | rep:{nb_responses:05d} | tot:{len(self.list_of_tasks):05d}')
                
                if cursor < len(self.list_of_tasks) and nb_running_tasks < max_nb_running_tasks:
                    generic_current_task = self.list_of_tasks[cursor]
                    self.tasks_states[generic_current_task.task_id] = {}
                    self.tasks_responses[generic_current_task.task_id] = {}

                    for topic in generic_current_task.topics:
                        if topic in self.map_topic2nb_switchs and self.map_topic2nb_switchs[topic] > 0:
                            nb_subscribers = self.map_topic2nb_switchs[topic]
                            specialized_task = SpecializedTask(
                                topic=topic, 
                                task_id=generic_current_task.task_id, 
                                task_content=generic_current_task.task_content 
                            )

                            self.source2switch_socket.send_string(topic, flags=zmq.SNDMORE)
                            self.source2switch_socket.send_pyobj(specialized_task)
                            nb_running_tasks += nb_subscribers 

                            logger.debug(f'{topic} has {nb_subscribers} target switch_solvers')
                        else:
                            logger.debug(f'{topic} has no target subcribers | job {generic_current_task.task_id} was not processed')
                            self.tasks_states[generic_current_task.task_id][topic] = TaskStatus.FAILED
                            self.tasks_responses[generic_current_task.task_id][topic] = []  
                            
                    cursor = cursor + 1
            except KeyboardInterrupt:
                keep_loop = False  
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop ...!


        end = perf_counter()
        duration = end - start 

        print(self.tasks_responses)
        
        logger.debug(f'all workers have finished their jobs | duration : {duration}')
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()

        logger.debug('waiting for switch to disconnect')
        worker_process.join()

        logger.debug(f'nb tasks : {len(self.tasks_responses)}')
        return 0 
        
    def __start_worker(self) -> None:
        worker = PRLRNRSwitch(
            source_liveness=self.source_liveness, 
            shutdown_signal=self.shutdown_signal,
            switch_start_loop=self.switch_start_loop,
            source2switch_address=self.source2switch_address,
            switch2source_address=self.switch2source_address,
            switch_solver_address=self.switch_solver_address,
            list_of_switch_configs=self.worker_config.list_of_switch_configs
        )

        with worker as wrk:
            wrk.running()  

    def __enter__(self):  
        try:
            self.source2switch_socket.bind(self.source2switch_address)
            self.switch2source_socket.bind(self.switch2source_address)
            self.zmq_initialized = 1
        except Exception as e:
            logger.error(e)
            return self 
        self.is_ready = 1
        return self 

    def __exit__(self, exc_type, exc_value, traceback):
        if self.zmq_initialized:
            self.source2switch_socket.close()
            self.switch2source_socket.close()
        self.ctx.term()
