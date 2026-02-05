#! python3
from typing import Any, List, Dict, Protocol, Union  # , Optional
from abc import ABC, abstractmethod


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        raise ValueError(f'Not implemented yet ! (class: {self.__class__})')


class InputStage(ProcessingStage):
    def process(self, data: Any) -> Dict:
        return {}


class TransformStage(ProcessingStage):
    def process(self, data: Any) -> Dict:
        return {}


class OutputStage(ProcessingStage):
    def process(self, data: Any) -> str:
        return ''


class ProcessingPipeline(ABC):
    def __init__(self, pipline_id: str) -> None:
        self.id: str = pipline_id
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    @abstractmethod
    def process(self, data) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        print('Processing JSON data through pipeline...')
        value: Dict = {'data': data, 'type': 'JSON'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break

        return value


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        print('Processing JSON data through pipeline...')
        value: Dict = {'data': data, 'type': 'CSV'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break
        return value


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        print('Processing JSON data through pipeline...')
        value: Dict = {'data': data, 'type': 'STREAM'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break
        return value


class NexusManager():
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipline(self, pipline: ProcessingPipeline) -> None:
        self.pipelines.append(pipline)
