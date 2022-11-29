import click 
import random
 
from os import path
from glob import glob 

from time import perf_counter

from typing import List, Dict, Tuple 

from log import logger
from engine.concurrent import CCRServer, CCRRunner
from dataschema.task_schema import SpecializedTask, GenericTask, Priority
from dataschema.worker_schema import WorkerConfig, SwitchConfig
from strategies import IMGSolver, SHASolver

@click.command()
@click.option('--nb_workers', help='number of workers', type=int, default=2)
def concurrent_server(nb_workers:int):    
    worker_config = WorkerConfig(
        list_of_switch_configs=[
            SwitchConfig(
                topics=['JPG', 'JPEG', 'PNG'],
                nb_solvers=32, 
                solver=SHASolver(),
                service_name='hash-service'
            ), 
            SwitchConfig(
                topics=['PNG'],
                nb_solvers=4, 
                solver=IMGSolver(path2target_dir='cache/png'),
                service_name='imagecopy-service'
            ), 
            SwitchConfig(
                topics=['JPG', 'JPEG'],
                nb_solvers=4, 
                solver=IMGSolver(path2target_dir='cache/jpg'),
                service_name='imagecopy-service'
            )
        ],
        max_nb_running_tasks=128
    )
        
    runner = CCRServer(
        nb_workers=nb_workers,
        client_broker_address='ipc://client_broker.ipc',
        broker_worker_address='ipc://broker_worker.ipc',
        worker_config=worker_config
    )
    runner.running()


@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--nb_workers', help='number of workers', type=int, default=2)
def concurrent_runner(path2source_dir:str, nb_workers:int):    
    worker_config = WorkerConfig(
        list_of_switch_configs=[
            SwitchConfig(
                topics=['HASH'],
                nb_solvers=16, 
                solver=SHASolver(), 
                service_name='hash-service'
            ), 

            SwitchConfig(
                topics=['COPY'],
                nb_solvers=16, 
                solver=IMGSolver(path2target_dir='cache'), 
                service_name='imagecopy'
            ),
        ],
        max_nb_running_tasks=512
    )
        
    filepaths = sorted(glob(path.join(path2source_dir, '*')))
    list_of_tasks:List[GenericTask] = []
   
    for path2image in filepaths:
        topics = ['HASH', 'COPY']
        task = GenericTask(
            task_id=path2image, 
            topics=topics,
            task_content=path2image,
            priority=Priority.MEDIUM
        )

        list_of_tasks.append(task)
    
    runner = CCRRunner(
        list_of_tasks=list_of_tasks, 
        nb_workers=nb_workers, 
        worker_config=worker_config
    )
    runner.running()

    
    