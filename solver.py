
from dataschema import Task

from typing import (
    List, Tuple, Dict, Any, Optional
)

class ZMQSolver:
    def __init__(self) -> None:
        # simple initialization 
        # option assignment etc ...!
        pass
    
    def initialize(self) -> None:
        # heavy initialization 
        # database connection etc ...!
        # load deap learning model 
        pass 

    def process_message(self, task:Task ,*args:Any, **kwds:Any) -> Any:
        # this function is the strategy 
        # each solver will use it to process the task 
        pass  

    def __call__(self, task:Task, *args: Any, **kwds: Any) -> Any:
        return self.process_message(task, *args, **kwds)

 
