from enum import Enum 
from typing import List, Tuple, Any, Optional 
from pydantic import BaseModel 
from dataschema.task_schema import Topics, TaskResponse

from engine import ABCSolver

class WorkerStatus(bytes, Enum):
    BUSY:bytes=b'BUSY'
    FREE:bytes=b'FREE'
    QUIT:bytes=b'QUIT'
    RESP:bytes=b'RESP'

class SwitchConfig(BaseModel):
    service_name:str 
    topics:Topics
    solver:ABCSolver
    nb_solvers:int 

    class Config:
        arbitrary_types_allowed = True
    
class WorkerConfig(BaseModel):
    max_nb_running_tasks:int 
    list_of_switch_configs:List[SwitchConfig]  # nb_solvers_per_switch has to be in this option 
    
    class Config:
        arbitrary_types_allowed = True

class WorkerResponse(BaseModel):
    response_type:WorkerStatus
    response_content:Optional[TaskResponse]=None
    
    class Config:
        arbitrary_types_allowed = True 

    
