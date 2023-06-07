import asyncio
import json
import random
import time
import traceback
import os


def exist_or_create_path(path):
    """
    Прповерит существование пути из папок и создаст их если не найдёт.

    path: путь к папке (без имени файла в конце, это пажно).
    """
    if not path:
        return
    start_dir = path[0] if path[0] in (".", os.sep) else ""
    dirs = path.split(os.sep)
    for dir in dirs:
        if not dir or dir == ".":
            continue
        start_dir = os.path.join(start_dir, dir)
        os.path.exists(start_dir) or os.mkdir(start_dir)


def save_by_exception(exception: Exception, from_path: str, to_path: str, file_name: str):
    """
    Универасльный метод сортировки ошибок по типам - сохраняет каждый файл в отдельную
    папку с именем и описанием исключения в качестве имени папки.

    exception: исключение
    from_path: папка в которой лежит целевой файл вызвавший ошибку
    to_path: базовый путь к папке с ошибками.
    file_name: имя файла вызвавшего ошибку и подлежащего переносу.
    """
    if not file_name:
        return
    new_path = f"{exception.__class__.__name__} {exception}"
    new_path = "".join(
        (char for char in new_path if char.lower() in "abcdefghijklmnopqrstuvwxyz0123456789 -_()[]{}\"':,.;+=!")
    )
    new_path = os.path.join(to_path, new_path)
    exist_or_create_path(new_path)
    os.rename(os.path.join(from_path, file_name), os.path.join(new_path, file_name))


def save_json_data(name: str, data):
    """
    Сохраняет данные в формате json (учтите что не всё можно сохранить без
    написания специального класса или без предварительного переводжа данных
    в строку или список)
    """
    if name:
        with open(name, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def load_json_data(name: str, default=None):
    """
    Читает данные из json файла.

    name - имя файла.
    default - передаваемое значение по умолчанию если файла нет или он не читается.
    """
    try:
        with open(name) as f:
            return json.load(f)
    except:
        return default


class BaseTask:
    """Базовый класс для создания задачи для асинхронного выполнения.

    После самостоятельной инициализации класса необходимо выполнить
    инициализацию базового класса. Инициализация предполагает передачу
    именованного параметра 'name' для сохранения данных после выполнения
    задачи.

    Метод: async def task(self, data) - это ваш собственный метод который
    призван выполнять какую-то работу.

    Метод: def data_generator(self) - возвращает значения для каждого
    последующего запуска функции, не включая данные которые передаются в
    реальном времени.

    Метод: def append(self, data) - просто добавляет новые данные для вызова
    вне очереди генерации, эти значения будут переданы на исполнение в первую
    очередь.

    Метод: def save(self) - сохраняет данные из self.results на диск под
    именем файла self.name

    Метод: def logger(self, data) - пишется пользователем и выполняется перед
    каждым запуском новой асинхронной задачи. Предназначена для логирования
    или сохранения статистики.

    Метод: def exit(self) - выполняется перед самым выходом после абсолютно
    всех иных процедур, один раз. Возможность прибрать за собой или произвести
    какие-то дейцствия на последок.
    """

    def __init__(self, *args, **kwargs):
        """
        Данный метод призван сформировать исходные данные для анализа и дальнейшей обработки их в методе data_generator
        """
        random.seed(time.time() + id(self))
        self._id = random.randint(0, 2**32 - 1)
        self._data = []
        self._data_gen = None
        self._gen_is_empty = False
        self.results = self.__dict__.get("results", kwargs.get("results", []))
        self.name = self.__dict__.get("name", kwargs.get("name", f"{self.__class__.__name__}-{self._id}"))
        self._continue = True
        ...

    async def task(self, data):
        """Асинхронная задача которая выполняется в данном классе. Пишется пользователем."""
        print(f"{self._id} sleep: {data}")
        await asyncio.sleep(data)
        self.results.append(f"{self._id}: sleep: {data}")
        ...

    def logger(self, data):
        """Логгирует данные которые необходимо сохранять или распечатывает. Определяется пользователем."""
        ...

    def exit(self):
        """
        Выполняется перед завершением работы. В этот момент пользователь может каким-то образом прибраться
        за собой или сделать какие-либо действия перед полным закрытием класса. После этой функции уже ничего
        автоматически запускаться не будет.
        """
        ...

    def data_generator(self):
        """
        Генератор данных который выбирает данные из заранее сформированных списков входной информации.
        В частности можно перебирать списки и выдавать различные комбинации из их параметров...
        """
        ...
        while self._continue:
            yield random.random()*10

    def append(self, data):
        """
        Добавляет данные для обработки их вне очереди если таковые появились в процессе выполнения задачи.
        """
        self._data.append(data)

    def save(self, name: str = None, data=None):
        """
        Сохраняет полученные результаты из self.results в self.name файл.
        Но:
            Если передать name - сохранит под этим именем.
            Если передать data - сохранит именно эти данные вместо self.results.

        Внутренняя логика процесса асинхронной загрузки всегда будет вызывать self.save() без параметров.
        """
        if name is None:
            name = self.name
        if data is None:
            data = self.results
        save_json_data(name, data)

    @property
    def new_task(self):
        """Генерит новую задачу на выполнение с конкретными параметрами."""
        if self._data_gen is None:
            self._data_gen = self.data_generator()

        if self._data:
             data = self._data.pop()
        elif self._gen_is_empty:
            return
        else:
            try:
                data = next(self._data_gen)
            except StopIteration:
                self._gen_is_empty = True
                return
            except KeyboardInterrupt:
                self._gen_is_empty = True
                return

        self.logger(data)

        def task():
            return self.task(data)
        return task


class AsyncTaskRuner:
    """
    Основной класс для запуска асинхронных задачь из списка объектов BaseTask.
    Необходимо выполнить метод run со списком объектов класса BaseTask в качестве
    аргумента. Инициализируется двумя параметрами - количеством выполняемых задач
    и временем для автоматического сохранения в секундах.
    """
    async def save_result_by_time(self, task_class: BaseTask):
        """
        Каждые self.TIME_TO_SAVE секунд записываем результат, это сделано чтобы если на 50% расчёта произошла ошибка,
        резальтат не потерялся
        """
        while True:
            await asyncio.sleep(self.time_to_save)
            task_class.save()

    async def run_tasks_class_in_async_mode(self, tasks_gen, loop, dalay=0.1):  # data
        """
        Запускает любую асинхронную задачу из класса задачи использующую
        входные данные в режиме ограничения количества параллельно запущенных
        потоков. В асинхрноом цикле будет одновременно крутиться
        self.chunk_size задач пока есть данные в генераторе.
        Как только данные закончатся система дождётся завершения выполнения
        последней задачи и закончит работу сохранив все результаты.

        tasks_gen - генератор задачь который возвращает всё новые и новые задачи для выполнения.
        loop = asyncio.new_event_loop() | asyncio.get_event_loop()
        dalay = 0.1 - задержка между проверками статуса, выполняется только если в
        цикле нет ни одной завершенной задачи.
        """

        def move_task():
            task = tasks.pop()
            if count < len(tasks):
                tasks[count] = task

        tasks, count, continue_task = [], 0, True

        while continue_task:
            continue_task = continue_task and bool(count)
            if len(tasks) < self.chunk_size:
                try:
                    tasks.append(asyncio.ensure_future(next(tasks_gen)(), loop=loop))
                except StopIteration:
                    pass
                except KeyboardInterrupt:
                    return

            if len(tasks) > 0:
                try:
                    if tasks[count].done():
                        yield tasks[count].result()
                        move_task()
                    else:
                        count += 1
                except KeyboardInterrupt:
                    return
                except Exception as exc:
                    print('Возникла проблема при выполнении задачи, требуется логирование.', exc)
                    traceback.print_exc()
                    move_task()

                continue_task = True

            if count >= len(tasks):
                count = 0
                await asyncio.sleep(dalay)


    def __init__(self, chunk_size: int = 20, time_to_save: int = 5*60):
        """
        chunk_size - Количество одновременно запущенных задач
        time_to_save - время автоматического сохранения результатов
        """
        self.chunk_size = chunk_size
        self.time_to_save = time_to_save


    def get_task_from_tasks_list(self, task_list: list[BaseTask]):
        """
        Перебирает по кругу доступные задачи среди переданного списка классов и если ни один класс не способен
        сгенерировать новую задачу - завершает процесс.
        """
        index = errors = 0
        max_index = len(task_list)
        while True:
            task = task_list[index].new_task
            if task:
                errors = 0
                yield task
            else:
                errors += 1
            index = (index + 1) % max_index
            if errors >= max_index:
                return

    async def main(self, loop, tasks: list[BaseTask]):
        if self.chunk_size > 1:
            print('Скрипт запущен запущен в асинхронном режиме. Потоков:', self.chunk_size)
        else:
            print('Скрипт запущен в синхронном режиме, 1 поток.')

        count = 0
        start_time = chunk_time = time.time()

        if self.time_to_save:
            save_by_tyme_tasks = [asyncio.create_task(self.save_result_by_time(task)) for task in tasks]
        async for _ in self.run_tasks_class_in_async_mode(
                tasks_gen=self.get_task_from_tasks_list(tasks),
                loop=loop
        ):
            count += 1
            if count % self.chunk_size == 0:
                # Считаем время
                time_left = round(time.time() - start_time, 3)
                chunk_time = round(time.time() - chunk_time, 3)
                print(f"--- Время итерации: {chunk_time} сек. Прошло: {time_left} сек.")
                chunk_time = time.time()

        # когда все посчитано ещё раз, на всякий случай записываем результат.
        for task in tasks:
            task.save()
        if self.time_to_save:
            for task in save_by_tyme_tasks:
                task.cancel()
        for task in tasks:
            task.exit()

    def run(self, tasks: list[BaseTask]):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.main(loop, tasks))
        loop.close()
