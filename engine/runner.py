import multiprocessing as mp 


from engine.worker import ZMQWorker
from engine.broker import ZMQBroker
from dataschema.task_schema import ListOfTasks
from dataschema.worker_schema import WorkerConfig

from time import sleep 

from typing import List, Tuple, Any 
from log import logger 

class PRLRunner:
    """parallel runner"""
    def __init__(self):
        pass 

class CCRRunner:
    """concurrent runner """
    def __init__(self, nb_workers:int, client_broker_address:str, broker_worker_address:str, worker_config:WorkerConfig):
        assert nb_workers > 0 
        
        self.nb_workers = nb_workers
        self.worker_config = worker_config

        self.client_broker_address = client_broker_address
        self.broker_worker_address = broker_worker_address

    def running(self) -> None:
        try:
            broker_process = mp.Process(target=self.__start_broker, args=[])
            broker_process.start()
            
            sleep(0.1)

            worker_processes:List[mp.Process] = []
            for worker_id in range(self.nb_workers):
                worker_processes.append(
                    mp.Process(
                        target=self.__start_worker, 
                        args=[worker_id]
                    )
                )
                worker_processes[-1].start()
            # end for loop cursor 

            broker_process.join()    
            for process_ in worker_processes:
                process_.join()
            # end for loop processes  
        except KeyboardInterrupt: 
            pass 
        except Exception as e:
            logger.error(e)
    # end function running 
        
    def __start_worker(self, worker_id:int) -> None:
        worker = ZMQWorker(
            worker_id=worker_id, 
            broker_worker_address=self.broker_worker_address, 
            worker_config=self.worker_config
        )
        with worker as wrk:
            wrk.running()
    
    def __start_broker(self) -> None:
        broker = ZMQBroker(
            client_puller_address=self.client_broker_address, 
            pusher_worker_address=self.broker_worker_address
        )
        with broker as brk:
            brk.running()
   
# end zmqworker class