#! python3
from typing import Any, List, Optional, Dict, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(
                self,
                stream_id: str,
                data_type: str,
                stream_type: str
            ) -> None:
        self.__stream_id: str = stream_id
        self.__data_type: str = data_type
        self.__stream_type: str = stream_type
        print(f'Initializing {stream_type} Stream...')

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
                self,
                data_batch: List[Any],
                criteria: Optional[str] = None
            ) -> List[Any]:
        return [
            data
            for data in data_batch
            if criteria is None or criteria in data
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            'stream_id': self.__stream_id,
            'data_type': self.__data_type,
            'stream_type': self.__stream_type
        }

    def print_stats(self) -> None:
        stats = self.get_stats()
        print(f'Stream ID: {str(stats["stream_id"]).capitalize()}, '
              f'Type: {str(stats["data_type"]).capitalize()}, '
              f'Stream Type: {str(stats["stream_type"]).capitalize()}')

    def print_processing(self, batch_data: List[Any]) -> None:
        print(f'Processing {self.__stream_type} batch: {batch_data}')
        print(self.process_batch(batch_data))


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, 'Environmental Data', 'sensor')
        self.total_temp: float = 0
        self.temp_count: int = 0

    def process_batch(self, data_batch: List[str]) -> str:
        temps: List[float] = []

        for data in data_batch:
            try:
                if isinstance(data, str):
                    key, value = data.split(':')
                    if key not in ['temp', 'humidity', 'pressure']:
                        raise ValueError(
                            f'key {key} in {data} is not supported'
                        )

                    if key == 'temp':
                        temps.append(float(value))
                else:
                    raise ValueError(f'{data} has to be str.')
            except Exception as e:
                print(f'format error: {e}')

        n_temp: int = len(temps)
        avg_temp: str
        if n_temp == 0:
            avg_temp = 'undefined'
        else:
            avg_temp = f'{sum(temps) / n_temp:.1f}'

        return f'Sensor analysis: {len(data_batch)} readings processed, ' + \
               f'avg temp: {avg_temp}°C'


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, 'Financial Data', 'transaction')
        self.total_units: int = 0

    def process_batch(self, data_batch: List[str]) -> str:
        operations: List[int] = []

        for data in data_batch:
            try:
                if isinstance(data, str):
                    key, value = data.split(':')
                    if key not in ['sell', 'buy']:
                        raise ValueError(
                            f'key {key} in {data} is not supported'
                        )

                    if key == 'sell':
                        operations.append(int(value) * -1)
                    else:
                        operations.append(int(value))
                else:
                    raise ValueError(f'{data} has to be str.')
            except Exception as e:
                print(f'format error: {e}')

        return f'Transaction analysis: {len(operations)} ' + \
               f'operations, net flow: {sum(operations):+} units'


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, 'System Events', 'event')

    def process_batch(self, data_batch: List[str]) -> str:
        errors_count: int = 0

        for event_name in data_batch:
            try:
                if isinstance(event_name, str):
                    if event_name not in ['login', 'error', 'logout']:
                        raise ValueError(
                            f'Event {event_name} is not supported'
                        )

                    if event_name == 'error':
                        errors_count += 1
                else:
                    raise ValueError(f'{event_name} has to be str.')
            except Exception as e:
                print(f'format error: {e}')
        return f'Event analysis: {len(data_batch)} events, ' + \
               f'{errors_count} error detected'


class StreamProcessor:
    @staticmethod
    def process(stream: DataStream, data_batch: List[str]) -> None:
        stream.process_batch(data_batch)


if __name__ == '__main__':
    print('=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n')
    sensor: SensorStream = SensorStream(
        'SENSOR_001',
    )
    sensor.print_stats()
    sensor.print_processing(['temp:22.5', 'humidity:65', 'pressure:1013'])
    print('')

    transaction: TransactionStream = TransactionStream('TRANS_001')
    transaction.print_stats()
    transaction.print_processing(['buy:100', 'sell:150', 'buy:75'])
    print('')

    events: EventStream = EventStream('EVENT_001')
    events.print_stats()
    events.print_processing(['login', 'error', 'logout'])
    print('')

    print('=== Polymorphic Stream Processing ===\n')
    print('Processing mixed stream types through unified interface...\n')

    print('Batch: ')
    to_process: List[tuple[DataStream, List[str]]] = [
        (transaction, ['buy:100', 'sell:150', 'buy:75']),
        (events, ['login', 'error', 'logout']),
        (sensor, ['temp:22.5', 'humidity:65', 'pressure:1013'])
    ]

    for (stream, data) in to_process:
        print(f'- {stream.process_batch(data)}')

    filtered_data = transaction.filter_data(
        ['buy:100', 'sell:150', 'buy:75'], '150'
    )
    print('\nStream filtering active:')
    print(f'- high price (=150): {filtered_data}')
    filtered_data = events.filter_data(['login', 'error', 'logout'], 'error')
    print(f'- errors: {filtered_data.__len__()}')
    filtered_data = sensor.filter_data(
        ['temp:22.5', 'humidity:65', 'pressure:1013'], 'temp'
    )
    print(f'- has temp sensor: {filtered_data.__len__() >= 1}')
