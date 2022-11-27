import click 
import random
 
from os import path
from glob import glob 

from time import perf_counter


from log import logger
from engine.runner import CCRRunner
from dataschema.task_schema import SpecializedTask, GenericTask
from dataschema.worker_schema import WorkerConfig, SwitchConfig
from strategies import IMGSolver

@click.command()
@click.option('--nb_workers', help='number of workers', type=int, default=2)
def parallel_processing(nb_workers:int):    
    worker_config = WorkerConfig(
        switch_config=[
            SwitchConfig(
                topics=['JPG', 'JPEG'],
                nb_solvers=8, 
                solver=IMGSolver(path2target_dir='cache/jpg')
            ), 
            SwitchConfig(
                topics=['PNG'],
                nb_solvers=8, 
                solver=IMGSolver(path2target_dir='cache/png')
            )
        ],
        max_nb_running_tasks=128
    )
        
    start = perf_counter()
    runner = CCRRunner(
        nb_workers=nb_workers,
        client_broker_address='ipc://client_broker.ipc',
        broker_worker_address='ipc://broker_worker.ipc',
        worker_config=worker_config
    )
    runner.running()
    end = perf_counter()
    
    duration = end - start
    logger.debug(f'duration : {duration} seconds')

    