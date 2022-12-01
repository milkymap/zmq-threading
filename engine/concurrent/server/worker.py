import zmq 
import queue 
import pickle 
import threading

from time import sleep

from log import logger 
from typing import List, Tuple, Dict, Optional, Any, Sequence

from dataschema.task_schema import Topic, TaskStatus, GenericTask, SpecializedTask, TaskResponse, TaskResponseData
from dataschema.worker_schema import WorkerConfig, WorkerResponse, WorkerStatus

class CCRSRVWorker:
    "concurrent worker for server mode"
    def __init__(self, worker_id:int, broker_worker_address:str, worker_config:WorkerConfig):        
        assert len(worker_config.list_of_switch_configs) > 0  # use pydantic field

        self.ctx = zmq.Context()

        self.worker_id = worker_id 
        self.nb_switchs = len(worker_config.list_of_switch_configs)
        self.list_of_switch_configs = worker_config.list_of_switch_configs
        self.pusher_source_address = broker_worker_address

        self.nb_connected_switchs = 0

        # add srv:(server) as prefix to avoid collusion
        self.source2switch_address = f'inproc://source2switch_{worker_id:03d}'
        self.switch2source_address = f'inproc://switch2source_{worker_id:03d}'   
        self.switch_solver_address = f'inproc://switch_solver_{worker_id:03d}'
        
        self.ctx.setsockopt(zmq.LINGER, 0)  # global option for all sockets 

        self.nb_running_tasks = 0
        self.max_nb_running_tasks = worker_config.max_nb_running_tasks

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
        # end loop over number of switchs 
        
        self.map_topic2nb_switchs:Dict[Topic, int] = {}

    def source(self) -> Optional[int]:
        try:
            source2switch_socket:zmq.Socket = self.ctx.socket(zmq.PUB)
            source2switch_socket.bind(self.source2switch_address)

            pusher_source_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            pusher_source_socket.connect(self.pusher_source_address) 

            switch2source_socket:zmq.Socket = self.ctx.socket(zmq.PULL)
            switch2source_socket.bind(self.switch2source_address)
        except Exception as e:
            logger.error(e) 
            return -1
        
        logger.success('source has initialized its zeromq ressources')
        self.source_switch_condition.acquire()
        logger.success('source is waiting for switchs to connect')
        self.source_liveness.set()  # notify all switch that the source is up 
        returned_value = self.source_switch_condition.wait_for(
            predicate=lambda: self.nb_connected_switchs == self.nb_switchs, 
            timeout=10
        )

        if not returned_value:
            logger.warning('source wait too long for switchs to connect')
            source2switch_socket.close()
            pusher_source_socket.close()
            switch2source_socket.close()
            return -1 
        
        poller = zmq.Poller()
        poller.register(pusher_source_socket, zmq.POLLIN)
        poller.register(switch2source_socket, zmq.POLLIN)

        logger.debug('all switchs are connected to the source')
        print(self.map_topic2nb_switchs)
        has_asked_a_job = False 
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
                
            if not has_asked_a_job:
                if self.nb_running_tasks < self.max_nb_running_tasks:
                    pusher_source_socket.send_string('', flags=zmq.SNDMORE)
                    pusher_source_socket.send_pyobj(
                        WorkerResponse(
                            response_type=WorkerStatus.FREE,  # notify pusher that we are free, 
                            response_content=None 
                        )
                    )
                    has_asked_a_job = True 
            
            try:
                map_socket2event = dict(poller.poll(timeout=100))
                
                switch2source_event = map_socket2event.get(switch2source_socket, None)
                if switch2source_event is not None:
                    if switch2source_event == zmq.POLLIN: 
                        response_from_switch:WorkerResponse = switch2source_socket.recv_pyobj()
                        assert response_from_switch.response_type == WorkerStatus.RESP
                        pusher_source_socket.send_string('', flags=zmq.SNDMORE)
                        pusher_source_socket.send_pyobj(response_from_switch)  # response_type can be either : FREE or RESP 
                        
                        if response_from_switch.response_content is not None:
                            if response_from_switch.response_content.response_type in [TaskStatus.DONE, TaskStatus.FAILED]:
                                self.nb_running_tasks -= 1   
                                logger.debug(f'worker {self.worker_id:03d} ==> nb tasks : {self.nb_running_tasks:03d}')

                pusher_source_event = map_socket2event.get(pusher_source_socket, None)
                if pusher_source_event is not None:
                    if pusher_source_event == zmq.POLLIN:
                        _, pusher_encoded_mssage = pusher_source_socket.recv_multipart()
                        pusher_plain_message:GenericTask = pickle.loads(pusher_encoded_mssage)
                        task_id=pusher_plain_message.task_id 
                        task_content=pusher_plain_message.task_content         
                        for topic in pusher_plain_message.topics:
                            if topic in self.map_topic2nb_switchs and self.map_topic2nb_switchs[topic] > 0:
                                nb_subscribers = self.map_topic2nb_switchs[topic]
                                current_task = SpecializedTask(
                                    topic=topic, 
                                    task_id=task_id, 
                                    task_content=task_content 
                                )

                                source2switch_socket.send_string(topic, flags=zmq.SNDMORE)
                                source2switch_socket.send_pyobj(current_task)
                                self.nb_running_tasks += nb_subscribers  
                                logger.debug(f'{topic} has {nb_subscribers} target switch_solvers')
                            else:
                                logger.debug(f'{topic} has no target subcribers => job {task_id} was not processed')
                             
                                response2send_to_pusher = WorkerResponse(
                                    response_type=WorkerStatus.RESP,
                                    response_content=TaskResponse(
                                        response_type=TaskStatus.FAILED,
                                        response_content=TaskResponseData(
                                            topic=topic,
                                            task_id=task_id,
                                            data=None  # no data 
                                        )
                                    )
                                )
                                pusher_source_socket.send_string('', flags=zmq.SNDMORE)
                                pusher_source_socket.send_pyobj(response2send_to_pusher)

                        # end for loop over topics
                        has_asked_a_job = False  # can ask a new job to the pusher based on the nb_running_tasks
            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 
        
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()
        
        self.source_switch_condition.acquire()
        self.source_quitloop.set()  # notify all switch to disconnect their sockets 
        self.source_switch_condition.wait_for(
            predicate=lambda: self.nb_connected_switchs == 0
        )
        logger.debug('all switch are disconnected from the source')
        source2switch_socket.close()
        switch2source_socket.close()
        pusher_source_socket.close()
        
        logger.success('source has released its zeromq ressources')
        self.threads_flag.set()  # notify main thread to end the zeromq context
        return None  
    # end function source 
        
    def switch(self, switch_id:int) -> Optional[int]:
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

            switch2source_socket:zmq.Socket = self.ctx.socket(zmq.PUSH)
            switch2source_socket.connect(self.switch2source_address)

            logger.success(f'switch {switch_id:03d} has initialized its zeromq ressource') 
        except Exception as e:
            logger.error(e)
            return -1 
        
        available_solvers:List[bytes] = [] 
        
        try:
            self.list_of_switch_configs[switch_id].solver.initialize()  # this can throw error : catch it with custom exception
            self.switchs_barrier.wait(timeout=5)
            logger.debug(f'switch {switch_id:03d} pass the barrier')
            sleep(0.01)  # waiting 100ms 
        except threading.BrokenBarrierError:
            logger.warning(f'switch {switch_id:03d} wait too long at the barrier')
            switch_solver_socket.close()
            source2switch_socket.close()
            switch2source_socket.close()
            return -1 

        self.switch_solver_conditions[switch_id].acquire()
        logger.debug(f'switch {switch_id:03d} is waiting for solver to connect')
        self.switchs_liveness[switch_id].set()  # notify all solvers that belong to this group(switch_id) to start their loop 
        returned_value = self.switch_solver_conditions[switch_id].wait_for(
            predicate=lambda: self.switchs_nb_connected_solvers[switch_id] == self.list_of_switch_configs[switch_id].nb_solvers, 
            timeout=10
        )

        if not returned_value:
            logger.warning(f'solvers take too long time to connect to the switch {switch_id:03d}')
            switch_solver_socket.close()
            source2switch_socket.close()
            return -1 
        
        logger.debug(f'all solvers are connected to the switch {switch_id:03d}')
        
        list_of_topics:List[str] = self.list_of_switch_configs[switch_id].topics
        with self.source_switch_condition:
            logger.debug(f'switch {switch_id:03d} got the lock and will register to : {list_of_topics}')
            for topic in list_of_topics:
                source2switch_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                if topic not in self.map_topic2nb_switchs:
                    self.map_topic2nb_switchs[topic] = 0
                self.map_topic2nb_switchs[topic] += 1 
            self.nb_connected_switchs += 1             
            self.source_switch_condition.notify()  # wake up the source
            logger.debug(f'switch {switch_id:03d} is ready to process message')
        # free the unferlying lock so that, other switch can communicate with the source
        poller = zmq.Poller()
        poller.register(source2switch_socket, zmq.POLLIN)
        poller.register(switch_solver_socket, zmq.POLLIN)

        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            
            try:
                map_socket2event = dict(poller.poll(timeout=100))
                if len(available_solvers) > 0:
                    # check if there is an available solver 
                    # if yes then, pull data from source and send it to the solver 
                    source2switch_event = map_socket2event.get(source2switch_socket, None)
                    if source2switch_event is not None:
                        if source2switch_event == zmq.POLLIN:
                            popped_solver_address:bytes = available_solvers.pop(0) # fifo strategy 
                            message_from_source:List[bytes]
                            message_from_source = source2switch_socket.recv_multipart()
                            _, source_encoded_message = message_from_source  # ignore the topic 
                            switch_solver_socket.send_multipart([popped_solver_address, b''], flags=zmq.SNDMORE)
                            switch_solver_socket.send(source_encoded_message)
            
                switch_solver_event = map_socket2event.get(switch_solver_socket, None)
                if switch_solver_event is not None: 
                    if switch_solver_event == zmq.POLLIN: 
                        message_from_solver:List[bytes]
                        message_from_solver = switch_solver_socket.recv_multipart()
                        solver_address, _, solver_encoded_message = message_from_solver
                        solver_plain_message:WorkerResponse = pickle.loads(solver_encoded_message)
                        if solver_plain_message.response_type == WorkerStatus.FREE:
                            available_solvers.append(solver_address)  # add in fifo maner 
                        elif solver_plain_message.response_type == WorkerStatus.RESP:
                            switch2source_socket.send(solver_encoded_message)  # optimize this, do not decode for just accessing the response_type
                        else:
                            pass  # impossible due to pydantic schema valdiation   
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
            switch2source_socket.close()

            self.nb_connected_switchs -= 1
            self.source_switch_condition.notify()
            logger.success(f'switch {switch_id:03d} has released its zeromq ressource')
        # free the underlying lock 
        return None 
    # end function switch 
    
    def solver(self, switch_id:int, solver_id:int) -> Optional[int]:
        liveness = self.switchs_liveness[switch_id].wait(timeout=5)
        if not liveness:
            logger.warning(f'solver {solver_id:03d} wait too long to get the signal from {switch_id:03d}')
            return -1 
        
        logger.debug(f'solver {solver_id:03d} {switch_id:03d} has received the signal from the switch')
        try:
            switch_solver_socket:zmq.Socket = self.ctx.socket(zmq.DEALER)
            switch_solver_socket.connect(f'{self.switch_solver_address}_{switch_id:03d}')
        except Exception as e:
            logger.error(e)
            return -1
        
        logger.debug(f'solver {solver_id:03d} {switch_id:03d} has initialized its zeromq ressources')
        with self.switch_solver_conditions[switch_id]:
            self.switchs_nb_connected_solvers[switch_id] += 1 
            self.switch_solver_conditions[switch_id].notify()
            logger.debug(f'solver {solver_id:03d} {switch_id:03d} is ready to process messages')

        has_asked_a_job = False 
        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            
            if not has_asked_a_job:
                switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                switch_solver_socket.send_pyobj(
                    WorkerResponse(
                        response_type=WorkerStatus.FREE,
                        response_content=None 
                    )
                )
                has_asked_a_job = True 
                
            try:
                polled_event = switch_solver_socket.poll(timeout=100)
                if polled_event == zmq.POLLIN:
                    message_from_switch:List[bytes]
                    message_from_switch = switch_solver_socket.recv_multipart()
                    _, switch_encoded_message = message_from_switch  # ignore delimiter 
                    switch_plain_message:SpecializedTask = pickle.loads(switch_encoded_message)
                    
                    switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                    switch_solver_socket.send_pyobj(
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
                        solver_response = self.list_of_switch_configs[switch_id].solver(task=switch_plain_message)
                        task_status = TaskStatus.DONE 
                    except Exception as e:
                        error_message = f'{e}'
                        logger.error(error_message)
                        solver_response =error_message 
                        task_status = TaskStatus.FAILED
                    
                    switch_solver_socket.send_string('', flags=zmq.SNDMORE)
                    switch_solver_socket.send_pyobj(
                        WorkerResponse(
                            response_type=WorkerStatus.RESP,
                            response_content=TaskResponse(
                                response_type=task_status,
                                response_content=TaskResponseData(
                                    topic=switch_plain_message.topic,
                                    task_id=switch_plain_message.task_id, 
                                    data={
                                        self.list_of_switch_configs[switch_id].service_name: solver_response
                                    } 
                                )
                            )
                        )
                    )
                    has_asked_a_job = False  # the solver is free and can ask a new job  
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
            switch_solver_socket.close()
            self.switchs_nb_connected_solvers[switch_id] -= 1 
            self.switch_solver_conditions[switch_id].notify()
            logger.debug(f'solver {solver_id:03d} {switch_id:03d} has released its zeromq ressource')
        # free underlying lock  
        return None 
    # end function solver 
    
    def running(self):
        # run all threads :  
        try:    
            self.source_thread.start()
            for switch_thread in self.array_of_switch_thread:
                switch_thread.start()
            for solver_thread in self.array_of_solver_thread:
                solver_thread.start()
            
            # wait for threads to terminate their loops
            solver_thread.join()
            for switch_thread in self.array_of_switch_thread:
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
            self.array_of_switch_thread:List[threading.Thread] = []
            self.array_of_solver_thread:List[threading.Thread] = []
            
            for switch_id in range(self.nb_switchs):
                switch_thread = threading.Thread(target=self.switch, args=[switch_id])
                self.array_of_switch_thread.append(switch_thread)
                for solver_id in range(self.list_of_switch_configs[switch_id].nb_solvers):
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