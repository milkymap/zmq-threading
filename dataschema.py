from enum import Enum 
from typing import (
    List, Tuple, Dict, Optional, Callable, Any, Union  
)
from pydantic import BaseModel

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

