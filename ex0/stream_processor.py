#! python3
from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @property
    def type(self):
        raise NotImplementedError

    def __init__(self) -> None:
        print(f'Initializing {self.type} Processor...')

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f'Output: {result}'

    def print_validate(self, data: Any) -> bool:
        validate: bool = self.validate(data)
        if (validate):
            print(f'Validation: {self.type} data verified')
        else:
            print(f'Validation: {self.type} data can\'t be verified')
        return (validate)

    def print_process(self, data: Any) -> str:
        output: str = self.process(data)
        print(f'Processing data: {data}')
        return (output)

    def do_all(self, data: Any) -> None:
        output: str = self.print_process(data)
        self.print_validate(data)
        print(f'Output: {output}')
        pass


class NumericProcessor(DataProcessor):
    type = 'Numeric'

    def process(self, data: list[int]) -> str:
        if (self.validate(data)):
            count: int = len(data)
            total: int = sum(data)
            avg: float = (total / count)
            return f'Processed {count} numeric values, ' + \
                   f'sum={total}, avg={avg:.1f}'

        return 'ERROR'

    def validate(self, data: list[int]) -> bool:
        if (type(data) is list):
            for val in data:
                if (type(val) is not int):
                    return False
            return True
        return False


class TextProcessor(DataProcessor):
    type = 'Text'

    def process(self, data: str) -> str:
        if (self.validate(data)):
            words: int = data.count(" ")
            return f'Processed text: {len(data)} characters, {words} words'
        return 'ERROR'

    def validate(self, data: str) -> bool:
        return type(data) is str


class LogProcessor(DataProcessor):
    type = 'Log'

    def process(self, data: str) -> str:
        if 'ERROR:' in data:
            return '[ALERTE] ERROR level detected: ' + \
                   f'{data.replace("ERROR: ", "")}'
        else:
            return f'[INFO] log: {data}'

    def validate(self, data: str) -> bool:
        return type(data) is str and data.__contains__('ERROR: ' or 'SUCCES: ')


if __name__ == '__main__':
    print('=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n')
    NumericProcessor().do_all([1, 2, 3, 4, 5])
    print('')
    TextProcessor().do_all('Hello Nexus World')
    print('')
    LogProcessor().do_all('ERROR: Connection timeout')
    print('\n=== Polymorphic Processing Demo ===')
