from enum import Enum 
from typing import (
    List, Tuple, Dict, Optional, Callable, Any, Union  
)
from pydantic import BaseModel, Field

Topic = str
Topics = List[Topic] 
ListOfJobs = List[Tuple[int, Topics, Any]]  # list of (priority, [topics], message)

class SolverConfig(BaseModel):
    swtich_config:List[Tuple[Topics, Callable]]   
    nb_solvers_per_switch:int
    source2switch_address:str 
    switch_solver_address:str

class WorkerConfig(BaseModel):
    jobs:ListOfJobs  
    solver_config:SolverConfig

class Priority(int, Enum):
    LOW:int=2
    MEDIUM:int=1 
    HIGH:int=0 

class SolverStatus(bytes, Enum):
    BUSY:str=b'BUSY'
    FREE:str=b'FREE'
    
class TaskStatus(str, Enum):
    DONE:str='DONE'
    FAILED:str='FAILED'
    RUNNING:str='RUNNING'
    PENDING:str='PENDING'
    SCHEDULED:str='SCHEDULED'
    
class Task(BaseModel):
    task_id:str 
    topic:str 
    content:Any 



