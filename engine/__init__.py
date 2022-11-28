
from dataschema.task_schema import SpecializedTask
from typing import Any

from abc import ABC, abstractmethod

class ABCSolver(ABC):
    """abstract base class solver"""
    def __init__(self) -> None:
        # simple initialization 
        # option assignment etc ...!
        # use initialize() for heavy config such as : database connection or model loading
        pass
    
    @abstractmethod
    def initialize(self) -> None:
        # heavy initialization 
        # database connection etc ...!
        # load deap learning model 
        pass 
    
    @abstractmethod
    def process_message(self, task:SpecializedTask ,*args:Any, **kwds:Any) -> Any:
        # this function is the strategy 
        # each solver will use it to process the incoming task
        # it can be a strategy pattern for each registered topics  
        pass  

    def __call__(self, task:SpecializedTask, *args: Any, **kwds: Any) -> Any:
        # special method : class => callable 
        return self.process_message(task, *args, **kwds)

 
