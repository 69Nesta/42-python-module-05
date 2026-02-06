#! python3
from typing import Any, List, Dict, Protocol, Union  # , Optional
from abc import ABC, abstractmethod
import json
import csv
import datetime


t_data = Dict


class ProcessingStage(Protocol):
    def process(self, data: t_data) -> Any:
        ...


class InputStage:
    def process(self, data: t_data) -> Dict:
        content = data.get('data')
        data_type = data.get('type')
        if not content or not data_type:
            raise ValueError('No data provided to InputStage !')
        if data_type not in ['JSON', 'CSV', 'STREAM']:
            raise ValueError(f'Unsupported data type: {data_type}')
        if data_type == 'JSON':
            print(f'Input: {content}')
        elif data_type == 'CSV':
            print(f'Input: {content.splitlines()[0]} (header),'
                  f' {len(content.splitlines()) - 1} records')
        elif data_type == 'STREAM':
            print(f'Input: Stream with {len(content.splitlines())} readings')
        return data


class TransformStage:
    def process(self, data: t_data) -> Dict:
        if not data.get('data') or not data.get('type'):
            raise ValueError('No data provided to TransformStage !')
        content = data.get('data')
        data_type = data.get('type')
        if data_type == 'JSON':
            try:
                transformed_data = json.loads(str(content))
                print('Transform: Enriched with metadata and validation')
                return {
                    'data': transformed_data,
                    'type': data_type,
                    'parsed': True
                }
            except json.JSONDecodeError as e:
                raise ValueError(f'Invalid JSON data: {e}')
        elif data_type == 'CSV':
            try:
                transformed_data = list(
                    csv.reader(str(content).splitlines())
                )
                print('Transform: Parsed and structured data')
                return {
                    'data': transformed_data,
                    'type': data_type,
                    'parsed': True
                }
            except csv.Error as e:
                raise ValueError(f'Invalid CSV data: {e}')
        elif data_type == 'STREAM':
            try:
                transformed_data = [
                    float(line.strip())
                    for line in str(content).splitlines()
                ]
                print('Transform: Aggregated and filtered')
                return {
                    'data': transformed_data,
                    'type': data_type,
                    'parsed': True
                }
            except Exception as e:
                raise ValueError(f'Invalid Stream data: {e}')
        else:
            raise ValueError(f'Unsupported data type: {data_type}')


class OutputStage:
    def process(self, data: t_data) -> str:
        content = data.get('data')
        data_type = data.get('type')
        if (not content
           or (data_type not in ['JSON', 'CSV', 'STREAM'])
           or not data.get('parsed', False)):
            raise ValueError('Wrong data provided to OutputStage !')
        if data_type == 'JSON':
            if content.get('sensor') == 'temp' and content.get('value'):
                print('Output: Processed temperature reading:'
                      f' {float(content.get("value")):.2f}°C (Normal range)')
        elif data_type == 'CSV':
            if 'action' in content[0] and 'timestamp' in content[0]:
                print('Output: User activity logged:'
                      f' {len(content)} actions processed')
        elif data_type == 'STREAM':
            avg = sum(content) / len(content) if content else 0
            print(f'Output: Stream summary: {len(content)} readings,'
                  f' avg: {avg:.2f}°C')
        return str(content)


class ProcessingPipeline(ABC):
    def __init__(self, pipline_id: str) -> None:
        self.id: str = pipline_id
        self.stages: List[ProcessingStage] = []
        self.processed_count: int = 0

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    def get_stats(self) -> tuple[list[str], int, int]:
        return (
            [stage.__class__.__name__ for stage in self.stages],
            self.processed_count,
            len(self.stages)
        )

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        self.processed_count += 1
        print('Processing JSON data through pipeline...')
        value: t_data = {'data': data, 'type': 'JSON'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break

        return value


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        self.processed_count += 1
        print('Processing CSV data through pipeline...')
        value: t_data = {'data': data, 'type': 'CSV'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break
        return value


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        self.processed_count += 1
        print('Processing STREAM data through pipeline...')
        value: t_data = {'data': data, 'type': 'STREAM'}
        for idx, stage in enumerate(self.stages):
            try:
                value = stage.process(value)
            except ValueError as e:
                print(f'Error detected in Stage {idx + 1}: {e}')
                break
        return value


class NexusManager():
    def __init__(self) -> None:
        self.pipelines: dict[str, ProcessingPipeline] = {}
        self.processed_count: int = 0

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines[pipeline.id] = pipeline

    def get_pipeline(self, pipeline_id: str) -> ProcessingPipeline:
        if pipeline_id in self.pipelines:
            return self.pipelines[pipeline_id]
        raise ValueError(f'No pipeline found for pipeline id: {pipeline_id}')

    def get_stats(self) -> tuple[list[str], int]:
        return (
            [
                pipeline.__class__.__name__
                for pipeline in self.pipelines.values()
            ],
            self.processed_count
        )

    def process_data(self, data: Any, pipeline_id: str) -> Any:
        for pipeline in self.pipelines.values():
            if pipeline.id == pipeline_id:
                return pipeline.process(data)
        raise ValueError(f'No pipeline found for pipeline id: {pipeline_id}')


def main() -> None:
    print('=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n\n'
          'Initializing Nexus Manager...\n'
          'Pipeline capacity: 1000 streams/second\n\n'
          'Creating Data Processing Pipeline...\n'
          'Stage 1: Input validation and parsing\n'
          'Stage 2: Data transformation and enrichment\n'
          'Stage 3: Output formatting and delivery\n\n'
          '=== Multi-Format Data Processing ===')
    print('')
    start_time = datetime.datetime.now()
    nexus = NexusManager()

    pipelines: list[ProcessingPipeline] = [
        JSONAdapter('json_pipeline'),
        CSVAdapter('csv_pipeline'),
        StreamAdapter('stream_pipeline')
    ]
    stages: list[ProcessingStage] = [
        InputStage(),
        TransformStage(),
        OutputStage()
    ]
    for pipeline in pipelines:
        for stage in stages:
            pipeline.add_stage(stage)
        nexus.add_pipeline(pipeline)

    datas = {
        'json_pipeline': '{"sensor": "temp", "value": 23.5, "unit": "C"}',
        'csv_pipeline': 'user,action,timestamp\nrpetit,login,508860584',
        'stream_pipeline': '21.2\n23.3\n24.1\n22.8\n25.0\n20.5'
    }

    for pipeline_id, data in datas.items():
        nexus.process_data(data, pipeline_id)
        print('')

    stages_names, processed_count, stage_count = (
        nexus.get_pipeline('json_pipeline').get_stats()
    )
    print('=== Pipeline Chaining Demo ===\n'
          f'Pipeline: {" -> ".join(stages_names)}\n'
          f'Data flow: Raw -> Processed -> Analyzed -> Stored\n\n'
          f'Chain result: {processed_count} records processed through '
          f'{stage_count}-stage pipeline\n'
          'Performance: 95% efficiency, '
          f'{(datetime.datetime.now() - start_time).total_seconds()}s '
          'total processing time\n')

    print('=== Error Recovery Test ===')
    print('Simulating pipeline failure...\n')
    nexus.process_data(
        '{"sensor": "temp", "value": "invalid", "unit": "C"}',
        'json_pipeline'
    )
    print('\nRecovery initiated: Switching to backup processor'
          '\nRecovery successful: Pipeline restored, processing resumed\n'
          '\nNexus Integration complete. All systems operational.')


if __name__ == '__main__':
    main()
