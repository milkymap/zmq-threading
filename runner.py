import multiprocessing as mp 

from worker import ZMQWorker
from dataschema import WorkerConfig, SolverConfig, ListOfJobs

from typing import List, Tuple, Any 
from log import logger 

class ZMQRunner:
    def __init__(self, list_of_jobs:ListOfJobs, nb_workers:int, solver_config:SolverConfig):
        assert nb_workers > 0 
        assert len(list_of_jobs) >= nb_workers

        self.nb_workers = nb_workers
        self.list_of_jobs = list_of_jobs
        self.solver_config = solver_config

    def running(self):
        try:
            nb_jobs = len(self.list_of_jobs)
            nb_jobs_per_worker = nb_jobs // self.nb_workers

            workers_processes:List[mp.Process] = []
            for cursor in range(0, nb_jobs, nb_jobs_per_worker):
                jobs_partition = self.list_of_jobs[cursor:cursor+nb_jobs_per_worker]
                workers_processes.append(
                    mp.Process(
                        target=self.__start_worker, 
                        args=[jobs_partition]
                    )
                )
                workers_processes[-1].start()
            # end for loop cursor 
                
            for process_ in workers_processes:
                process_.join()
            # end for loop processes  
        except KeyboardInterrupt: 
            pass 
        except Exception as e:
            logger.error(e)
    # end function running 
        
    def __start_worker(self, list_of_jobs:ListOfJobs):
        worker_config = WorkerConfig(
            jobs=list_of_jobs, 
            solver_config=self.solver_config
        )
        worker = ZMQWorker(worker_config=worker_config)
        with worker as wrk:
            wrk.running()
    # end function start_worker 
            
# end zmqworker class