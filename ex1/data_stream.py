#! python3
from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStrem(ABC):
    def __init__(
                self,
                stream_id: str,
                data_type: str,
                stream_type: str
            ) -> None:
        self.__stream_id: str = stream_id
        self.__data_type: str = data_type
        self.__stream_type: str = stream_type

    @abstractmethod
    def process_batch(self, batch_data: List[Any]) -> str:
        pass

    def filter_data(
                self,
                data_batch: List[Any],
                criteria: Optional[str] = None
            ) -> List[Any]:
        return [
            data
            for data in data_batch
            if criteria is None or data == criteria
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            'stream_id': self.__stream_id.capitalize(),
            'data_type': self.__data_type,
            'stream_type': self.__stream_type
        }


class SensorStream(DataStrem):
    pass


if __name__ == '__main__':
    pass
