import zmq 

import multiprocessing as mp 

from typing import List, Optional
from dataschema.worker_schema import SwitchConfig
from engine.parallel.runner.solver import PRLRNRSolver

from log import logger 

class PRLRNRSwitch:
    def __init__(self, source_liveness:mp.Event, source2switch_address:str, array_of_switch_config:List[SwitchConfig]):
        self.ctx = zmq.Context() 
        self.array_of_switch_config = array_of_switch_config
        assert len(self.array_of_switch_config) > 0 

        self.source2switch_address = source2switch_address  # pub/sub communication 
        self.nb_switchs = len(self.array_of_switch_config)

        self.source_liveness:mp.Event = source_liveness
    
    def switch(self, switch_id:int):
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
        
        logger.debug(f'all solvers are connected to the switch {switch_id:03d}')
     


    def __enter__(self):
        # create nb_switchs threads 
        pass 

    def __exit__(self, exc_type, exc_value, traceback):
        pass 