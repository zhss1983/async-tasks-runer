import asyncio
import random

from support import AsyncTaskRuner, load_json_data, BaseTask

class BaseTaskExample(BaseTask):
    def __init__(self, *args, **kwargs):
        self.results = []
        super().__init__(*args, **kwargs)

    async def task(self, data):
        """Асинхронная задача которая выполняется в данном классе. Пишется пользователем."""
        print(f"{self._id} sleep: {data}")
        await asyncio.sleep(data)
        self.results.append(f"{self._id}: sleep: {data}")


class Task1(BaseTaskExample):
    def data_generator(self):
        """
        Генератор данных который выбирает данные из заранее сформированных списков входной информации.
        В частности можно перебирать списки и выдавать различные комбинации из их параметров...
        """
        for i in range(10):
            yield random.random() * i


class Task2(BaseTaskExample):
    def data_generator(self):
        """
        Генератор данных который выбирает данные из заранее сформированных списков входной информации.
        В частности можно перебирать списки и выдавать различные комбинации из их параметров...
        """
        ...
        for i in range(10):
            yield i


class Task3(BaseTaskExample):
    def data_generator(self):
        """
        Генератор данных который выбирает данные из заранее сформированных списков входной информации.
        В частности можно перебирать списки и выдавать различные комбинации из их параметров...
        """
        ...
        for i in range(50, 0, -1):
            yield i/10


tasks = [Task3(name="t3.json"), Task2(name="t2.json"), Task1(name="t1.json")]


if __name__ == "__main__":
    atr = AsyncTaskRuner(chunk_size=2, time_to_save=300)
    atr.run(tasks)