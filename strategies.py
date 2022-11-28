from os import path
from shutil import copyfile
from hashlib import sha256

from typing import Any 
from dataschema.task_schema import SpecializedTask

from engine import ABCSolver

class IMGSolver(ABCSolver):
    def __init__(self, path2target_dir:str) -> None:
        super(IMGSolver, self).__init__()
        self.path2target_dir = path2target_dir 
    
    def initialize(self) -> None:
        return super().initialize()

    def process_message(self, task:SpecializedTask, *args: Any, **kwds: Any) -> int:
        path2source_image:str = task.task_content
        if path.isfile(path2source_image):
            _, filename = path.split(path2source_image)
            path2target_image = path.join(self.path2target_dir, filename) 
            copyfile(path2source_image, path2target_image)
            return path2target_image 
        return FileNotFoundError(path2source_image) 

class SHASolver(ABCSolver):
    def __init__(self) -> None:
        super(SHASolver, self).__init__()
    
    def initialize(self) -> None:
        self.hasher = sha256
        return super().initialize()

    def process_message(self, task: SpecializedTask, *args: Any, **kwds: Any) -> str:
        with open(task.task_content, mode='rb') as fp:
            bytestream = fp.read()
            return self.hasher(bytestream).hexdigest()
