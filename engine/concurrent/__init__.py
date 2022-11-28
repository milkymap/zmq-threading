import multiprocessing as mp 


from dataschema.task_schema import ListOfTasks, GenericTask, Topic 
from dataschema.worker_schema import WorkerConfig
from engine.concurrent.server.worker import CCRSRVWorker
from engine.concurrent.runner.worker import CCRRNRWorker
from engine.broker import ZMQBroker

from math import ceil
from time import sleep, perf_counter

from typing import List, Tuple, Any, Dict 
from log import logger 

class CCRServer:
    """concurrent server"""
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
            
            sleep(0.1)  # wait 100ms 

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
        
    def __start_worker(self, worker_id:int) -> None:
        worker = CCRSRVWorker(
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
   
            
class CCRRunner:
    """concurrent runner"""
    def __init__(self, list_of_tasks:List[GenericTask], nb_workers:int, worker_config:WorkerConfig):
        assert len(list_of_tasks) > 1 
        assert nb_workers > 0

        self.list_of_tasks = list_of_tasks
        self.nb_workers = nb_workers 
        self.worker_config = worker_config 
        self.worker_responses_queue:mp.Queue[Dict[str, Dict[Topic, Any]]] = mp.Queue()
        self.worker_barrier = mp.Barrier(parties=self.nb_workers)

    def running(self):
        try:
            nb_tasks = len(self.list_of_tasks)
            nb_tasks_per_worker = ceil(nb_tasks / self.nb_workers)

            worker_id = 0
            worker_processes:List[mp.Process] = []
            for cursor in range(0, nb_tasks, nb_tasks_per_worker):
                partition_of_tasks = self.list_of_tasks[cursor:cursor+nb_tasks_per_worker]
                
                worker_processes.append(
                    mp.Process(
                        target=self.__start_worker, 
                        args=[partition_of_tasks, worker_id]
                    )
                )
                worker_id += 1 
            assert len(worker_processes) == self.nb_workers

            for process in worker_processes:
                process.start()
            
            start = perf_counter()
            keep_loop = True 
            while keep_loop:
                if self.worker_responses_queue.qsize() == self.nb_workers:
                    keep_loop = False 
                sleep(0.05)
            
            end = perf_counter()
            duration = end - start 
            
            while not self.worker_responses_queue.empty():
                response_from_worker = self.worker_responses_queue.get()
                print(response_from_worker)
            
            logger.debug(f'all workers have finished their jobs | duration : {duration}')
            
        except KeyboardInterrupt:
            pass 
        except Exception as e:
            logger.error(e)

    def __start_worker(self, parition_of_tasks:List[GenericTask], worker_id:int) -> None:
        worker = CCRRNRWorker(
            worker_id=worker_id, 
            list_of_tasks=parition_of_tasks,
            worker_config=self.worker_config, 
            worker_responses_queue=self.worker_responses_queue, 
            worker_barrier=self.worker_barrier
        )

        with worker as wrk:
            wrk.running()
