from os import path
from shutil import copyfile

from typing import Any 
from dataschema import Task

from solver import ZMQSolver

class IMGSolver(ZMQSolver):
    def __init__(self, path2target_dir:str) -> None:
        super(IMGSolver, self).__init__()
        self.path2target_dir = path2target_dir 
    
    def initialize(self) -> None:
        return super().initialize()

    def process_message(self, task:Task, *args: Any, **kwds: Any) -> int:
        path2source_image:str = task.content
        if path.isfile(path2source_image):
            _, filename = path.split(path2source_image)
            path2target_image = path.join(self.path2target_dir, filename) 
            copyfile(path2source_image, path2target_image)
            return 1 
        return 0 
