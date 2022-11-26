import click 
import random
 
from os import path
from glob import glob 
from time import time, sleep 


from log import logger
from worker import ZMQWorker
from runner import ZMQRunner
from dataschema import Task, SolverConfig

from strategies import IMGSolver

@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, dir_okay=True))
@click.option('--nb_workers', help='number of workers', type=int, default=2)
@click.option('--nb_solvers_per_switch', help='number of solvers per switch', type=int, default=4)
def parallel_processing(path2source_dir:str, nb_workers:int, nb_solvers_per_switch:int):
    image_paths = sorted(glob(path.join(path2source_dir, '*')))
    extensions = []
    priorities = []
    for path2image in image_paths:
        priorities.append(0)
        _, filename = path.split(path2image)
        extension:str = filename.split('.')[-1]
        extensions.append([extension.upper()])

    solver_config = SolverConfig(
        swtich_config=[
            (['JPG', 'JPEG'], IMGSolver('cache/jpg')), 
            (['PNG'], IMGSolver('cache/png')), 
            (['PDF', 'TXT'], IMGSolver('cache/pdf')), 
        ], 
        nb_solvers_per_switch=nb_solvers_per_switch,  # this has to be an option for swtich 
        source2switch_address='inproc://source2switch', 
        switch_solver_address='inproc://switch_solver'
    )
        
    start = time()
    
    runner = ZMQRunner(
        list_of_jobs=list(zip(priorities, extensions, image_paths)),
        nb_workers=nb_workers, 
        solver_config=solver_config
    )

    runner.running()

    end = time()
    duration = int((end - start) * 1000) 
    logger.debug(f'duration : {duration} ms')

    