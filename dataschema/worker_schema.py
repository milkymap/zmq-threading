from enum import Enum 
from typing import List, Tuple, Any, Optional 
from pydantic import BaseModel 
from dataschema.task_schema import Topics, TaskResponse

from engine.solver import ABCSolver

class WorkerStatus(bytes, Enum):
    BUSY:bytes=b'BUSY'
    FREE:bytes=b'FREE'
    QUIT:bytes=b'QUIT'
    RESP:bytes=b'RESP'

class SwitchConfig(BaseModel):
    topics:Topics
    solver:ABCSolver
    nb_solvers:int 

    class Config:
        arbitrary_types_allowed = True
    
class WorkerConfig(BaseModel):
    switch_config:List[SwitchConfig]  # nb_solvers_per_switch has to be in this option 
    max_nb_running_tasks:int 

    class Config:
        arbitrary_types_allowed = True

class WorkerResponse(BaseModel):
    response_type:WorkerStatus
    response_content:Optional[TaskResponse]=None
    
    class Config:
        arbitrary_types_allowed = True 

    
