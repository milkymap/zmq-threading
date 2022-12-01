import zmq 
import pickle 
import threading
import multiprocessing as mp 

from time import sleep, time 

from multiprocessing.synchronize import Event, Condition, Barrier

from typing import List, Dict 
from dataschema.worker_schema import SwitchConfig, WorkerResponse, WorkerStatus
from dataschema.task_schema import Topic
from engine.parallel.runner.solver import PRLRNRSolver

from log import logger 

class PRLRNRSwitch:
    def __init__(self, source_liveness:Event, shutdown_signal:Event, switch_start_loop:Event, source2switch_address:str, switch2source_address:str, switch_solver_address:str, list_of_switch_configs:List[SwitchConfig]):
        assert len(list_of_switch_configs) > 0 

        self.ctx = zmq.Context() 
        self.ctx.setsockopt(zmq.LINGER, 0)
        
        self.nb_switchs:int = len(list_of_switch_configs)
        self.source_liveness:Event = source_liveness
        self.switch_start_loop:Event = switch_start_loop
        self.list_of_switch_configs:List[SwitchConfig] = list_of_switch_configs

        self.shutdown_signal:Event = shutdown_signal 
        self.solver_start_loop:Event = mp.Event()
        
        self.source2switch_address:str = source2switch_address  # pub/sub 
        self.switch2source_address:str = switch2source_address  # push/pull 
        self.switch_solver_address:str = switch_solver_address  # router/dealer 

        self.switch2source_socket:zmq.Socket 
        self.list_of_source2switch_sockets:List[zmq.Socket] = []
        self.list_of_switch_solver_sockets:List[zmq.Socket] = []

        self.nb_ready_switchs = 0
        self.start_sync_solvers = threading.Event()
        self.synchronizer_condition = threading.Condition() 
        self.map_topic2nb_switchs:Dict[Topic, int] = {}
    
        self.list_of_solvers:List[List[bytes]] = []
        self.list_of_switch_liveness:List[Event] = []
        self.list_of_solver_barriers:List[Barrier] = []

        self.is_ready = 0
        self.zmq_initialized = 0 

    def create_sockets(self) -> int:
        try:
            # initialize push socket 
            self.switch2source_socket = self.ctx.socket(zmq.PUSH)
            self.switch2source_socket.connect(self.switch2source_address)
            
            for switch_id in range(self.nb_switchs):
                # initialze router socket 
                switch_solver_socket:zmq.Socket = self.ctx.socket(zmq.ROUTER)
                switch_solver_socket.bind(self.switch_solver_address.replace('.ipc', f'_{switch_id}.ipc'))
                self.list_of_switch_solver_sockets.append(switch_solver_socket)

                # initialize subscriber socket 
                source2switch_socket:zmq.Socket = self.ctx.socket(zmq.SUB)
                source2switch_socket.connect(self.source2switch_address)
                self.list_of_source2switch_sockets.append(source2switch_socket)

                
                self.list_of_switch_liveness.append(mp.Event())
                self.list_of_solver_barriers.append(mp.Barrier(
                    parties=self.list_of_switch_configs[switch_id].nb_solvers
                    )
                )

                logger.success(f'switch {switch_id:03d} has initialized its zeromq ressource') 
            # end for loop ...!
            
            self.poller = zmq.Poller()
            # register a poller to subscriber socket 
            for socket in self.list_of_source2switch_sockets:
                self.poller.register(socket, zmq.POLLIN)
            
            # register a poller to router socket 
            for socket in self.list_of_switch_solver_sockets:
                self.poller.register(socket, zmq.POLLIN)
            
            # initialize all solvers list for each switch 
            for switch_id in range(self.nb_switchs):
                self.list_of_solvers.append([])

            logger.success('all switch sockets were created')
            self.zmq_initialized = 1
            return 0 

        except Exception as e:
            logger.error(e)
        return 1 

    def destroy_sockets(self) -> int:
        try:
            # unregister poller to subscriber socket 
            for socket in self.list_of_source2switch_sockets:
                self.poller.unregister(socket)
            
            # unregister poller to router socket 
            for socket in self.list_of_switch_solver_sockets:
                self.poller.unregister(socket)
            
            # close connection : subscriber socket
            for socket in self.list_of_source2switch_sockets:
                socket.close()

            # close connection : router socket 
            for socket in self.list_of_switch_solver_sockets:
                socket.close()  
            self.switch2source_socket.close()

            logger.debug('switch sockets were disconnected')
            return 0
        except Exception as e:
            logger.error(e)
        return 1

    
    def syncrhonize(self, switch_id:int) -> None:
        self.start_sync_solvers.wait(timeout=5)  # wait the signal from main thread 
        self.list_of_switch_liveness[switch_id].set()  # notify solvers to start the event loop 
        
        start = time()
        counter = 0
        keep_loop = True 
        while keep_loop:
            if counter == self.list_of_switch_configs[switch_id].nb_solvers:
                keep_loop = False 
            polled_event = self.list_of_switch_solver_sockets[switch_id].poll(100)
            if polled_event == zmq.POLLIN: 
                _, _, solver_encoded_message = self.list_of_switch_solver_sockets[switch_id].recv_multipart()
                solver_plain_message:WorkerResponse = pickle.loads(solver_encoded_message)
                if solver_plain_message.response_type == WorkerStatus.JOIN:
                    counter = counter + 1
            end = time()
            duration = end - start 
            if duration > 10:  # timeout after 10s : make it an argument to the class  
                keep_loop = False 
        # end while loop 

        if counter == self.list_of_switch_configs[switch_id].nb_solvers:
            with self.synchronizer_condition:
                list_of_topics:List[str] = self.list_of_switch_configs[switch_id].topics
                for topic in list_of_topics:
                    self.list_of_source2switch_sockets[switch_id].setsockopt_string(zmq.SUBSCRIBE, topic) 
                    if topic not in self.map_topic2nb_switchs:
                        self.map_topic2nb_switchs[topic] = 0 
                    self.map_topic2nb_switchs[topic] += 1

                logger.debug(f'switch {switch_id:03d} is ready to process message')
                self.nb_ready_switchs += 1 # one switch is ready 
                self.synchronizer_condition.notify()  # notify the main thread to check the condition 
        else:
            logger.debug(f'switch {switch_id:03d} was not able to make syncrhonization with solvers')
            
    def running(self) -> int:
        if not self.is_ready:
            return 1 

        solver_processes:List[mp.Process] = []
        try:
            for switch_id in range(self.nb_switchs):
                for solver_id in range(self.list_of_switch_configs[switch_id].nb_solvers):
                    process_ = mp.Process(
                        target=self.__start_solver,
                        kwargs={
                            'switch_id': switch_id, 
                            'solver_id': solver_id, 
                            'solver_barrier': self.list_of_solver_barriers[switch_id] 
                        }
                    ) 
                    solver_processes.append(process_)
            # end loop over switch_ids 
            
            for process_ in solver_processes:
                process_.start()
        except Exception as e:
            logger.error(e)
            return 1 

        syncrhonizer_threads:List[threading.Thread] = []
        try:
            for switch_id in range(self.nb_switchs):
                thread_ = threading.Thread(
                    target=self.syncrhonize,
                    args=[switch_id]
                )
                syncrhonizer_threads.append(thread_)
            
            for thread_ in syncrhonizer_threads:
                thread_.start()

        except Exception as e:
            logger.error(e)
            return 1 

        self.synchronizer_condition.acquire()
        self.start_sync_solvers.set()  # notify all syncrhonizers to start 
        returned_value = self.synchronizer_condition.wait_for(
            predicate=lambda: self.nb_ready_switchs == self.nb_switchs, 
            timeout=10
        )

        for thread_ in syncrhonizer_threads:
            thread_.join()

        if not returned_value:
            logger.debug('switchs were not able to make syncrhonization')
            return 1  

        self.switch2source_socket.send_pyobj(self.map_topic2nb_switchs)
        
        # signal from source to continue 
        returned_value = self.switch_start_loop.wait(timeout=5)
        if not returned_value:
            if not self.shutdown_signal.is_set():
                self.shutdown_signal.set()

        self.solver_start_loop.set()  # solver can start their loop 

        keep_loop = True 
        while keep_loop:
            if self.shutdown_signal.is_set():
                keep_loop = False 
            try:
                map_socket2event = dict(self.poller.poll(timeout=100))
                for switch_id in range(self.nb_switchs):
                    if len(self.list_of_solvers[switch_id]) > 0:
                        source2switch_polled_event = map_socket2event.get(self.list_of_source2switch_sockets[switch_id], None)
                        if source2switch_polled_event is not None:
                            if source2switch_polled_event == zmq.POLLIN:
                                popped_solver_address = self.list_of_solvers[switch_id].pop(0)
                                message_from_source:List[bytes] = self.list_of_source2switch_sockets[switch_id].recv_multipart()
                                _, source_encoded_message = message_from_source
                                self.list_of_switch_solver_sockets[switch_id].send_multipart(
                                    [popped_solver_address, b'', source_encoded_message]
                                ) 
                    
                    switch_solver_polled_event = map_socket2event.get(self.list_of_switch_solver_sockets[switch_id], None)
                    if switch_solver_polled_event is not None: 
                        if switch_solver_polled_event == zmq.POLLIN: 
                            message_from_solver:List[bytes] = self.list_of_switch_solver_sockets[switch_id].recv_multipart()
                            solver_address, _, solver_encoded_message = message_from_solver
                            solver_plain_message:WorkerResponse = pickle.loads(solver_encoded_message)
                            if solver_plain_message.response_type == WorkerStatus.FREE:
                                self.list_of_solvers[switch_id].append(solver_address)
                            elif solver_plain_message.response_type == WorkerStatus.RESP:
                                self.switch2source_socket.send_pyobj(solver_plain_message)
                            else:  # impossible due to pydantic validation 
                                pass 
                # end for loop over switch_ids 

            except KeyboardInterrupt:
                keep_loop = False 

            except Exception as e:
                logger.error(e)
                keep_loop = False 
        # end while loop 
        
        if not self.shutdown_signal.is_set():
            self.shutdown_signal.set()

        logger.debug(f'switchs are waiting for solvers to quit')
        for process_ in solver_processes:
            process_.join()
        
        return 0 
        # end while loop 
        
    def __start_solver(self, switch_id:int, solver_id:int, solver_barrier:Barrier):
        switch_liveness = self.list_of_switch_liveness[switch_id]
        solver_strategy = self.list_of_switch_configs[switch_id].solver
        service_name = self.list_of_switch_configs[switch_id].service_name

        solver = PRLRNRSolver(
            solver_id=solver_id,
            switch_id=switch_id,
            service_name=service_name,
            solver_start_loop=self.solver_start_loop,
            solver_barrier=solver_barrier,
            switch_liveness=switch_liveness,
            shutdown_signal=self.shutdown_signal,
            solver_strategy=solver_strategy,
            swith_solver_address=self.switch_solver_address, 
        )

        with solver as slv:
            slv.running()

    def __enter__(self):
        liveness_returned_value = self.source_liveness.wait(timeout=5)
        if not liveness_returned_value:
            logger.debug(f'switchs wait too long to get the signal from source')
            return self 

        sockets_creation_returned_value:int = self.create_sockets()
        if sockets_creation_returned_value != 0: 
            return self

        self.is_ready = 1 
        return self 

    def __exit__(self, exc_type, exc_value, traceback):    
        if self.zmq_initialized:
            self.destroy_sockets()

        logger.debug('all switchs were disconnected')
        self.ctx.term()
        