import requests
from loguru import logger
from datetime import datetime


class YandexDiskConnector:
    def __init__(self, token, cloud_folder):
        self.token = token
        self.cloud_folder = cloud_folder
        self.base_url = "https://cloud-api.yandex.net/v1/disk"
        self.headers = {
            "Authorization": f"OAuth {token}"
        }

    def _create_folder(self):
        """Создаёт папку в облаке."""
        url = f"{self.base_url}/resources"
        params = {"path": self.cloud_folder}
        response = requests.put(url, headers=self.headers, params=params)
        if response.status_code in [201, 202]:
            return True
        else:
            logger.error(f"Failed to create folder: {response.status_code}")
            return False

    def _get_upload_link(self, filename):
        """Получает ссылку для загрузки файла."""
        url = f"{self.base_url}/resources/upload"
        params = {
            "path": f"{self.cloud_folder}/{filename}",
            "overwrite": "true"
        }
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json().get("href")
            else:
                logger.error(f"Failed to get upload link for {filename}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error getting upload link for {filename}: {e}")
            return None

    def load(self, local_path):
        """Загружает файл в облако."""
        filename = local_path.split("/")[-1]
        upload_url = self._get_upload_link(filename)
        if not upload_url:
            return False

        try:
            with open(local_path, "rb") as file:
                response = requests.put(upload_url, data=file)
            if response.status_code == 201:
                logger.info(f"File {filename} uploaded successfully")
                return True
            else:
                logger.error(f"Upload failed for {filename}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error uploading {filename}: {e}")
            return False

    def reload(self, local_path):
        """Перезагружает файл (аналогично загрузке)."""
        return self.load(local_path)

    def delete(self, filename):
        """Удаляет файл из облака."""
        url = f"{self.base_url}/resources"
        params = {"path": f"{self.cloud_folder}/{filename}"}
        try:
            response = requests.delete(url, headers=self.headers, params=params)
            if response.status_code == 204:
                logger.info(f"File {filename} deleted successfully")
                return True
            elif response.status_code == 404:
                # Файл уже удалён — считаем операцию успешной
                logger.info(f"File {filename} already deleted (404 Not Found)")
                return True
            else:
                logger.error(f"Delete failed for {filename}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error deleting {filename}: {e}")
            return False

    def get_info(self):
        """Возвращает информацию о файлах в облачной папке."""
        url = f"{self.base_url}/resources"
        params = {"path": self.cloud_folder, "fields": "items.name,items.modified"}
        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code == 200:
            files_info = {}
            items = response.json().get("_embedded", {}).get("items", [])
            for item in items:
                # Обрабатываем дату с учётом смещения часового пояса
                cloud_time_str = item["modified"]
                # Убираем смещение часового пояса (+00:00), оставляем только основную часть
                cloud_time_clean = cloud_time_str.split('+')[0]
                # Парсим дату без миллисекунд и смещения
                cloud_time = datetime.strptime(cloud_time_clean, "%Y-%m-%dT%H:%M:%S")
                files_info[item["name"]] = cloud_time.timestamp()
            return files_info
        elif response.status_code == 404:
            # Папка не найдена — создаём её
            logger.warning(f"Folder {self.cloud_folder} not found, creating...")
            create_response = self._create_folder()
            if create_response:
                logger.info(f"Folder {self.cloud_folder} created successfully")
                return self.get_info()
            else:
                logger.error(f"Failed to create folder {self.cloud_folder}")
                return {}
        else:
            logger.error(f"Failed to get cloud files info: {response.status_code} - {response.text}")
            return {}

# def test_connection():
#     """Тестирует подключение к Яндекс Диску."""
#     connector = YandexDiskConnector(
#         token="ваш_токен",
#         cloud_folder="sync_folder"
#     )
#     info = connector.get_info()
#     print("Cloud files:", info)

# Раскомментируйте для теста:
# test_connection()
