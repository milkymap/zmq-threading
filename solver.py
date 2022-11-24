
from typing import (
    List, Tuple, Dict, Any, Optional
)

class ZMQStrategy:
    def __init__(self) -> None:
        pass
    
    def process_message(self, *args:Any, **kwds:Any) -> Any:
        pass

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.process_message(*args, **kwds)

 
