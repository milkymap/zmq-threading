from enum import Enum 
from typing import (
    List, Tuple, Dict, Optional, Callable, Any, Union  
)
from pydantic import BaseModel, Field

class Priority(int, Enum):
    LOW:int=2
    MEDIUM:int=1 
    HIGH:int=0 

Topic = str
Topics = List[Topic] 
ListOfTasks = List[Tuple[Priority, Topics, Any]]  # list of (priority, [topics], message)
    
class TaskStatus(str, Enum):
    DONE:str='DONE'
    FAILED:str='FAILED'
    RUNNING:str='RUNNING'
    
class GenericTask(BaseModel):
    task_id:str 
    priority:Priority
    topics:Topics
    task_content:Any 

class SpecializedTask(BaseModel):
    topic:str 
    task_id:str 
    task_content:Any 

class TaskResponseData(BaseModel):
    task_id:str
    topic:Topic 
    data:Any=None  

class TaskResponse(BaseModel):
    response_type:TaskStatus
    response_content:TaskResponseData 

    class Config:
        arbitrary_types_allowed = True




