import traceback
import bz2
import gzip
import gpxpy
import json
from math import atan2, pi
import aiohttp
import os

from support import AsyncTaskRuner, load_json_data, BaseTask, save_by_exception, exist_or_create_path, save_json_data


class UploadGpxFile(BaseTask):
    RAD_TO_GRAD = 180 / pi
    BASE_PATH = "output/"
    GOOD_PATH = "good/"
    OTHER_PATH = "other/"
    ERROR_PATH = "error/"
    ERROR_URL_PATH = "error/url/"
    ERROR_GPX_PATH = "error/gpx/"

    def __init__(self, *args, **kwargs):
        exist_or_create_path(self.GOOD_PATH)
        exist_or_create_path(self.ERROR_PATH)
        exist_or_create_path(self.ERROR_GPX_PATH)
        exist_or_create_path(self.ERROR_URL_PATH)
        self.name_delete = "deleted_files.json"
        self.name = "new_description.json"
        self.name_full_dict = "description.json"
        self.results_for_search = load_json_data(self.name_full_dict, {}) | load_json_data(self.name, {})
        self.results = {}
        self._deleted = load_json_data(self.name_delete, [])

        # Пока True - данные для обработки есть.
        # Как только станет False - больше обрабатывать нечего.
        self._continue = True

        # Для вывода логов
        self._count_good = 0
        self._count_points = 0
        super().__init__(*args, **kwargs)

    def source_list(self):
        gpx_files = set(os.listdir(self.BASE_PATH))
        paths = []
        for gpx_file in gpx_files:
            if os.path.isfile(self.BASE_PATH + gpx_file) and ".gpx" in gpx_file:
                paths.append(gpx_file)
        return paths

    def load_file(self, file: str) -> [str, str]:
        base_path_file = os.path.join(self.BASE_PATH, file)
        if not os.path.exists(base_path_file):
            return None, file
        if file.endswith(".gpx"):
            with open(base_path_file, 'rb') as f:
                data = f.read()
            return data, file
        if file.endswith(".bz2"):
            with bz2.open(base_path_file, 'rb') as f:
                data = f.read()
            with open(base_path_file[:-4], 'wb') as f:
                f.write(data)
            os.remove(base_path_file)
            return data, file[:-4]
        if file.endswith(".gz"):
            with gzip.open(self.BASE_PATH + file, 'rb') as f:
                data = f.read()
            with open(base_path_file[:-3], 'wb') as f:
                f.write(data)
                os.remove(base_path_file)
            return data, file[:-3]
        return None, file

    def moov_error_file(self, file: str):
        os.rename(self.BASE_PATH + file, self.ERROR_PATH + file)

    def logger(self, data):
        if self._count_good == 1000:
            print(f"Всего загружено в БД: {self._count_points} точек.")
            self._count_good = 0

    async def task(self, data):
        payload, file_name = data
        base_url = os.getenv("URL")
        auth = os.getenv("BASIC_AUTH_GPS")
        url = base_url + "<special method name>/"

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth
        }
        try:
            timeout = aiohttp.ClientTimeout(total=24*60*60)
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False),
                    raise_for_status=True,
            ) as session:
                async with session.post(url, headers=headers, data=payload, timeout=timeout) as resp:
                    if resp.status == 201:
                        self._count_good += 1
                        content = int(await resp.content.read())
                        if content:
                            self._count_points += int(content)
                            self._continue = True
                            if os.getenv("SAVE_MODE", True) == "False":
                                os.remove(self.BASE_PATH + file_name)
                                self._deleted.append(file_name)
                            else:
                                os.rename(self.BASE_PATH + file_name, self.GOOD_PATH + file_name)
                            return
        except KeyboardInterrupt:
            self._continue = False
            return
        except Exception as exc:
            print(exc)
            traceback.print_exc()
            save_by_exception(exc, self.BASE_PATH, self.ERROR_PATH, file_name)
            return
        # Исключения не возникло, но что-то пошло не так.
        os.rename(self.BASE_PATH + file_name, self.ERROR_URL_PATH + file_name)

    def save(self, name: str = None, data=None):
        super().save(name=name, data=data)
        super().save(self.name_delete, self._deleted)

    def data_generator(self):
        while self._continue:
            self._continue = False
            files = self.source_list()
            for file in files:
                data, file = self.load_file(file)
                if data is None:
                    self.moov_error_file(file)
                    continue

                track_id = file.split(".gpx")[0]
                if not track_id.isdigit():
                    self.moov_error_file(file)
                    continue

                try:
                    gpx = gpxpy.parse(data)
                except KeyboardInterrupt:
                    return
                except Exception as exc:
                    print(exc)
                    traceback.print_exc()
                    # save_by_exception сама рассортирует различные ошибки по папкам.
                    save_by_exception(exc, self.BASE_PATH, self.ERROR_GPX_PATH, file)
                    continue
                points = []
                data = {"id": int(track_id), "points": points}

                self.results[track_id] = []
                for track in gpx.tracks:
                    print(f"Название трека: {track.name}")
                    self.results[track_id].append(track.name)
                    for segment in track.segments:
                        segment_len = len(segment.points) - 1
                        for index in range(len(segment.points)):
                            point = segment.points[index]

                            if index < segment_len:
                                dlat = segment.points[index + 1].latitude - point.latitude
                                dlng = segment.points[index + 1].longitude - point.longitude
                            else:
                                dlat = point.latitude - segment.points[index-1].latitude
                                dlng = point.longitude - segment.points[index-1].longitude


                            speed = segment.get_speed(index)
                            if speed is None:
                                continue
                            angle = (atan2(dlng, dlat) * self.RAD_TO_GRAD) % 360
                            dot = {
                                "lat": point.latitude,
                                "lng": point.longitude,
                                "timestamp": str(point.time.strftime("%Y-%m-%d %H:%M:%S.%f")),
                                "speed": speed,
                                "angle": angle
                            }
                            points.append(dot)
                if not points:
                    self.moov_error_file(file=file)
                    continue
                yield json.dumps(data), file

    def exit(self):
        """
        Набор инструкций выполняемых по завершению:
            Объединил новый список со старым и сохранил его.
            Удалил временный файл.
            Распечатал результат работы.
        """
        self.save(self.name_full_dict, self.results | self.results_for_search)
        os.path.exists(self.name) and os.remove(self.name)
        print(f"Добавлено {self._count_points} новых записей.")


tasks = [UploadGpxFile()]


if __name__ == "__main__":
    atr = AsyncTaskRuner(chunk_size=int(os.getenv("CHUNK_SIZE", 10)), time_to_save=300)
    atr.run(tasks)
