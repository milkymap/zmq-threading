from os import path
from shutil import copyfile
from hashlib import sha256, md5

from PIL import Image 
from typing import Any, Union
from dataschema.task_schema import SpecializedTask

from sentence_transformers import SentenceTransformer 

from engine import ABCSolver

class IMGSolver(ABCSolver):
    def __init__(self, path2target_dir:str) -> None:
        super(IMGSolver, self).__init__()
        self.path2target_dir = path2target_dir 
    
    def initialize(self) -> None:
        return super().initialize()

    def process_message(self, task:SpecializedTask, *args: Any, **kwds: Any) -> Union[str, Exception]:
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

class MD5Solver(ABCSolver):
    def __init__(self) -> None:
        super(MD5Solver, self).__init__()
    
    def initialize(self) -> None:
        self.hasher = md5 
        return super().initialize()

    def process_message(self, task: SpecializedTask, *args: Any, **kwds: Any) -> str:
        with open(task.task_content, mode='rb') as fp:
            bytestream = fp.read()
            return self.hasher(bytestream).hexdigest()


class STFSolver(ABCSolver):
    """class that will load the transformer model and extract embedding"""
    def __init__(self, transformer_name, cache_dir) -> None:
        super(STFSolver, self).__init__()
        self.transformer_name = transformer_name
        self.cache_dir = cache_dir 
        
    def initialize(self) -> None:
        self.transformer = SentenceTransformer(
            model_name_or_path=self.transformer_name, 
            cache_folder=self.cache_dir
        )
    
    def process_message(self, task: SpecializedTask, *args: Any, **kwds: Any) -> Any:
        pil_image = Image.open(task.task_content)
        embedding = self.transformer.encode(pil_image, device='cpu')
        return embedding

