import click 
import random
 
from os import path
from glob import glob 

from time import perf_counter

from typing import List, Dict, Tuple 

from log import logger
from engine.concurrent import CCRServer, CCRRunner
from engine.parallel import PRLServer, PRLRunner

from dataschema.task_schema import SpecializedTask, GenericTask, Priority
from dataschema.worker_schema import WorkerConfig, SwitchConfig
from strategies import IMGSolver, SHASolver, MD5Solver, STFSolver

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
                service_name='sha-hash'
            ), 

            SwitchConfig(
                topics=['HASH'],
                nb_solvers=16, 
                solver=MD5Solver(), 
                service_name='md5-hash'
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

    
@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--nb_workers', help='number of workers', type=int, default=2)
def parallel_runner(path2source_dir:str, nb_workers:int):    
    worker_config = WorkerConfig(
        list_of_switch_configs=[
            SwitchConfig(
                topics=['Embedding'],
                nb_solvers=2,               
                solver=STFSolver(transformer_name='clip-ViT-B-32', cache_dir='cache/transformers'),
                service_name='embedding'
            )
        ],
        max_nb_running_tasks=512
    )
        
    filepaths = sorted(glob(path.join(path2source_dir, '*')))
    list_of_tasks:List[GenericTask] = []
      
    for path2image in filepaths:
        topics = ['Embedding']
        task = GenericTask(
            task_id=path2image, 
            topics=topics,
            task_content=path2image,
            priority=Priority.MEDIUM
        )

        list_of_tasks.append(task)
    
    runner = PRLRunner(
        list_of_tasks=list_of_tasks[:128],
        worker_config=worker_config,
        switch2source_address='ipc://parallel_switch2source.ipc',
        source2switch_address='ipc://parallel_source2switch.ipc',
        switch_solver_address='ipc://parallel_switch_solver.ipc'
    )
    with runner as rnr:
        rnr.running()

    
    