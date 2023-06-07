import json
import aiohttp
import traceback
import os

from bs4 import BeautifulSoup
from support import AsyncTaskRuner, load_json_data, BaseTask, exist_or_create_path, save_json_data
from shapely.geometry import shape, Point


class CheckNewPages(BaseTask):
    """
    Скачивает новые ссылки на GPS треки пользователей с сайта
    https://www.openstreetmap.org/
    """

    def __init__(self, append, *args, **kwargs):
        """
        self.results - новые ссылки (self.name - передаётся в kwargs) найденные во время поиска.
        В идеале этот файл будет отсутствовать при шатном завершении скрипта. Всё сохранит в
        файл self.name_full_list ("page_links.json").
        self.results_for_search - ранее загруженные ссылки. Файл self.name_full_list
        ("page_links.json")
        Забил на переачу имени для сохранения результатов, вписал по хардкору.
        """
        self.name = "new_page_list.json"
        self.name_full_list = "page_links.json"
        self.results_for_search = set(load_json_data(self.name_full_list, [])) | set(load_json_data(self.name, []))
        self.results = set()
        self._results_len = len(self.results)

        # Пока True - данные для обработки есть.
        # Как только станет False - больше обрабатывать нечего.
        self._continue = True

        self.mail_to = append  # Метод для добавления полученного результата в другой обработчик.
        self._count_good = 0  # Для вывода логов
        super().__init__(*args, **kwargs)

    def logger(self, data):
        """Печатает каждые 1000 новых ссылок статистику (количество найденных)."""
        if self._count_good == 1000:
            print(f"Всего найдено: {len(self.results)} ссылок.")
            self._count_good = 0

    async def task(self, data):
        """
        Скачивает с https://www.openstreetmap.org/ страницу со ссылками на GPS треки.
        Разбирает её на составляющие и выгружает новые ссылки типа:
        https://www.openstreetmap.org/user/dragonpilot/traces/7706595
        """
        url = "https://www.openstreetmap.org/traces/page/%s"
        try:
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False),
                    raise_for_status=True,
            ) as session:
                async with session.get(url % data, timeout=timeout) as resp:
                    if resp.status != 200:
                        return

                    # Блок отвечающий за поиск и добавление новых ссылок со страницы.
                    soup = BeautifulSoup(await resp.content.read(), "html.parser")
                    a_links = soup.find_all('a', href=True)
                    for a in a_links:
                        if a.text.endswith(".gpx") and a["href"] not in self.results_for_search:
                            self.results.add(a["href"])
                            self.mail_to(a["href"])
                            self._count_good += 1

                    # Блок отвечающий за проверку на факт добавления новых ссылок.
                    new_len = len(self.results)
                    if self._results_len == new_len:
                        self._continue = False
                        print("!!! STOP ITERATIONS !!!")
                    else:
                        self._results_len = new_len
        except KeyboardInterrupt:
            self._continue = False
        except Exception as exc:
            print(exc)
            traceback.print_exc()

    def data_generator(self):
        """
        Генератор: требуется перебирать страницы с первой по ... чем дальше тем больше...
        Но практика такова что на текущем этапе требуется загрузить всего ничего - от
        1 до 100 новых страниц с новыми треками - остальные уже загружены ранее.
        """
        count = 1
        while self._continue:
            yield count
            count += 1

    def save(self,  name: str = None, data=None):
        """
        Сохраняет полученные результаты из self.results в self.name файл.
        Позволяет так же сохранить произвольные данные в произавольный файл.
        """
        if name is None:
            name = self.name
        if data is None:
            data = self.results
        save_json_data(name, list(data))

    def exit(self):
        """
        Набор инструкций выполняемых по завершению:
            Объединил новый список со старым и сохранил его.
            Удалил временный файл.
            Распечатал результат работы.
        """
        self.save(self.name_full_list, self.results | self.results_for_search)
        os.path.exists(self.name) and os.remove(self.name)
        print(f"Добавлено {self._results_len} новых записей. Всего: {len(self.results)}.")


class CheckNewLinks(BaseTask):
    """
    Данный класс передназначен для проверки стриницы загрузки трека.
    Проверяется принадлежит ли трек РФ. Те что приндлежат - сохраняются со статусом 1.

    self._links_for_search - исходный набор ссылок для проверки координат.
    Ищу те что принадлежат РФ.
    self._results_for_search - ранее обработанные ссылки.
    self.results - новые проверенные ссылки.
        <0 - ошибка, необходимо повторить попытку (число - это количество попыток).
        0 - не РФ, можно забить на страницу.
        1 - РФ, в дальнейшую обработку.
        2 - РФ, уже обработана, более не обращать внимания.
    self._errors_links - список не правильных ссылок - id на странице не
    совпадает с id в ссылке. В идеале таковых быть не должно.
    """

    def __init__(self, append, *args, **kwargs):
        """
        Забил на переачу имени для сохранения результатов, вписал по хардкору.
        """
        self.name = "new_rus_links.json"
        self.name_full_dict = "rus_links.json"
        self.name_input_data = "page_links.json"
        self.name_errors = "error_links.json"
        self._links_for_search = load_json_data(self.name_input_data, [])
        self._results_for_search = load_json_data(self.name_full_dict, {}) | load_json_data(self.name, {})
        self.results = {}
        russian_duration_json = load_json_data("russia.duration.json", {})
        self._russian_duration_polygon = shape(russian_duration_json['geometry'])
        self._errors_links = load_json_data(self.name_errors, [])
        self.mail_to = append
        self._count_good = 0
        self._continue = True
        super().__init__(*args, **kwargs)

    def logger(self, data):
        """Логирует данные которые необходимо сохранять или распечатывает. Определяется пользователем."""
        if self._count_good == 10:
            print(f"Найдено: {len(self.results)} треков в пределах РФ.")
            self._count_good = 0

    async def task(self, data: str):
        def decrement():
            self.results[data_id] = self.results.get(data_id, 0) - 1

        url = "https://www.openstreetmap.org%s"
        data_id = data.split("/")[-1]
        try:
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False),
                    raise_for_status=True,
            ) as session:
                async with session.get(url % data, timeout=timeout) as resp:
                    if resp.status != 200:
                        return
                    soup = BeautifulSoup(await resp.content.read(), "html.parser")
                    table = soup.find("table")  # Выбор таблицы на странице
                    if not table:
                        decrement()
                        return
                    # Блок проверки и получения ссылки
                    url_link = table.find("a", href=True)
                    if not url_link:
                        decrement()
                        return
                    if url_link["href"] != f"/trace/{data_id}/data":
                        decrement()
                        self._errors_links.append((data, url_link["href"]))
                        return

                    # Блок получения координат
                    lat = table.find("span", {"class": "latitude"})
                    if not lat:
                        decrement()
                        return
                    lng = table.find("span", {"class": "longitude"})
                    if not lng:
                        decrement()
                        return
                    lat = float(lat.text.replace(",", "."))
                    lng = float(lng.text.replace(",", "."))

                    # Блок проверки на вхождение в РФ
                    # point = Point(lat, lng)
                    point = Point(lng, lat)
                    if self._russian_duration_polygon.contains(point):
                        self.results[data_id] = 1
                        self.mail_to(data_id)
                        self._count_good += 1
                    else:
                        self.results[data_id] = 0
                    return
        except KeyboardInterrupt:
            self._continue = False
        except Exception as exc:
            print(exc)
            traceback.print_exc()
        decrement()

    def data_generator(self):
        """Генератор: Перебирает все ссылки которые сохранил CheckNewPages
        и проверяю их статус в self._links_for_search. Если статуса нет или
        была ошибка загрузки - повторяю процесс загрузки (передаю ссылку)."""
        link: str
        for link in self._links_for_search:
            if not self._continue:
                return
            link_id = link.split("/")[-1]
            if -10 <= self._results_for_search.get(link_id, -2) < 0:
                yield link

    def save(self, name: str = None, data=None):
        super().save(name, data)
        save_json_data(self.name_errors, self._errors_links)

    def exit(self):
        """
        Набор инструкций выполняемых по завершению:
            Объединил новый словарь со старым и сохранил его.
            Удалил временный файл.
            Распечатал результат работы.
        """
        # Новый словарь перепишет значения в старом.
        all_recs = self._results_for_search | self.results
        self.save(self.name_full_dict, all_recs)
        os.path.exists(self.name) and os.remove(self.name)
        print(f"Проверено: {len(self.results)} записей.")
        print(f"Всего: {len(all_recs)}.")
        print(f"Найдено новых треков: {len([value for value in self.results.values() if value == 1])}.")


class DownloadGpxFile(BaseTask):
    """
    Метод призван выкачивать все файлы которые содержат треки находящиеся на территории РФ.

    Названия файлов захардкодил.

    self._gpx_to_download - список всех файлов на закачивание.
    self._results_for_search - все ранее загруженные файлы.
    self.results - список новых файлов которые были загружены во время текущей сессии.
    self._errors_links - список файлов с которыми возникли ошибки
    """
    def __init__(self, *args, **kwargs):
        """
        Забил на переачу имени для сохранения результатов, вписал по хардкору.
        """
        self.name = "new_gpx_id.json"
        self.name_full_set = "gpx_id.json"
        self.name_input_data = "rus_links.json"
        self.name_errors = "error_gpx.json"
        exist_or_create_path("output")
        self._gpx_to_download = [key for key, value in load_json_data(self.name_input_data, {}).items() if value == 1]
        self._results_for_search = set(load_json_data(self.name_full_set, [])) | set(load_json_data(self.name, []))
        self.results = set()
        self._errors_links = load_json_data(self.name_errors, [])
        self._count_good = 0
        super().__init__(*args, **kwargs)
        self._continue = True

    def logger(self, data):
        """Печатает результаты загрузки после каждого 10 загруженного файла."""
        if self._count_good == 10:
            print(f"Загружено: {len(self.results)} треков.")
            self._count_good = 0

    async def task(self, data: str):
        """Выполняет загрузку файла."""
        url = "https://www.openstreetmap.org/trace/%s/data"
        try:
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False),
                    raise_for_status=True,
            ) as session:
                async with session.get(url % data, timeout=timeout) as resp:
                    if resp.status != 200:
                        return
                    content = await resp.content.read()
                    filename = resp.content_disposition.filename
                    with open(f"output/{filename}", "wb") as file:
                        file.write(content)
                    self.results.add(data)
                    self._count_good += 1
                    return
        except KeyboardInterrupt:
            self._continue = False
        except Exception as exc:
            print(exc)
            traceback.print_exc()
        self._errors_links.append(data)

    def data_generator(self):
        """Генератор: Перебирает все id которые сохранил CheckNewLinks
        и если ранее с таким id файл не грузился - возвращает этот id."""
        for gpx_id in self._gpx_to_download:
            if not self._continue:
                return
            if gpx_id not in self._results_for_search:
                yield gpx_id

    def save(self, name: str = None, data=None):
        """
        Сохраняет полученные результаты из self.results в self.name файл.
        Сохраняет ошибки из self._errors_links в self.name_errors файл.
        Позволяет так же сохранимть произвольные данные в произвольныфй файл.
        """
        if name is None:
            name = self.name
        if data is None:
            data = self.results
        save_json_data(name, list(data))
        save_json_data(self.name_errors, self._errors_links)

    def exit(self):
        """
        Набор инструкций выполняемых по завершению:
            Объединил новый сет со старым и сохранил его.
            Удалил временный файл.
            Распечатал результат работы.
        """
        all_recs = self._results_for_search | self.results
        self.save(self.name_full_set, all_recs)
        os.path.exists(self.name) and os.remove(self.name)
        print(f"Загружено: {len(self.results)} файлов.")
        print(f"Всего: {len(all_recs)}.")


load_gpx = DownloadGpxFile()
chk_links = CheckNewLinks(append=load_gpx.append)  # Из метода CheckNewLinks будет вызываться метод load_gpx.append
chk_page = CheckNewPages(append=chk_links.append)  # Из метода CheckNewPages будет вызываться метод chk_links.append

tasks = [chk_page, chk_links, load_gpx]

if __name__ == "__main__":
    atr = AsyncTaskRuner(chunk_size=20, time_to_save=300)
    atr.run(tasks)
