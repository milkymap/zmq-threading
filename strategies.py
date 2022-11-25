import click 
import random
 
from os import path
from glob import glob 
from time import time, sleep 
from shutil import copyfile

from log import logger
from worker import ZMQWorker
from solver import ZMQSolver

class ZMQImageCopy(ZMQSolver):
    def __init__(self, path2target_dir:str) -> None:
        super(ZMQImageCopy, self).__init__()
        self.path2target_dir = path2target_dir 

    def process_message(self, path2source_image:str) -> int:
        if path.isfile(path2source_image):
            _, filename = path.split(path2source_image)
            path2target_image = path.join(self.path2target_dir, filename) 
            copyfile(path2source_image, path2target_image)
            return 1 
        return 0 

@click.command()
@click.option('--path2source_dir', help='path to source files', type=click.Path(exists=True, dir_okay=True))
@click.option('--nb_solvers_per_switch', help='number of solvers per switch', type=int, default=4)
def start_worker(path2source_dir:str, nb_solvers_per_switch:int):
    image_paths = sorted(glob(path.join(path2source_dir, '*')))
    extensions = []
    priorities = []
    for path2image in image_paths:
        priorities.append(0)
        _, filename = path.split(path2image)
        extension:str = filename.split('.')[-1]
        extensions.append([extension.upper()])

    start = time()
    worker = ZMQWorker(
        jobs=list(zip(priorities, extensions, image_paths)),
        swtich_config=[
            (['JPG', 'JPEG'], ZMQImageCopy('cache/jpg')), 
            (['PNG'], ZMQImageCopy('cache/png')), 
        ], 
        nb_solvers_per_switch=nb_solvers_per_switch,  # this has to be an option for swtich 
        source2switch_address='inproc://source2switch', 
        switch_solver_address='inproc://switch_solver'
    )
    worker.start_all_threads()
    end = time()
    duration = int((end - start) * 1000) 
    logger.debug(f'duration : {duration} ms')

    