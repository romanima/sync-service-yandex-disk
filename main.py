import os
import time
from dotenv import dotenv_values
from loguru import logger
from cloud_connector import YandexDiskConnector


def load_config():
    """Загружает конфигурацию из .env файла."""
    config = dotenv_values(".env")
    required_keys = ["LOCAL_FOLDER_PATH", "CLOUD_FOLDER_NAME", "YANDEX_DISK_TOKEN", "SYNC_INTERVAL", "LOG_FILE_PATH"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration: {key}")

    return config


def setup_logger(log_file_path):
    """Настраивает логгер."""
    logger.add(
        log_file_path,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )
    logger.info("Logger initialized")


def sync_files(connector, local_folder, logger):
    """Синхронизирует файлы между локальной папкой и облаком."""
    if not os.path.exists(local_folder):
        logger.error(f"Local folder does not exist: {local_folder}")
        return

    try:
        cloud_files = connector.get_info()
        local_files = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]

        logger.info(f"Local files: {local_files}")
        logger.info(f"Cloud files: {list(cloud_files.keys())}")

        # 1. Загружаем новые файлы (которых нет в облаке)
        for local_file in local_files:
            if local_file not in cloud_files:
                local_path = os.path.join(local_folder, local_file)
                logger.info(f"Uploading new file: {local_file}")
                connector.load(local_path)

        # Обновляем список файлов в облаке после загрузки
        cloud_files = connector.get_info()

        # 2. Обновляем изменённые файлы
        for local_file in local_files:
            if local_file in cloud_files:
                local_path = os.path.join(local_folder, local_file)
                local_mtime = os.path.getmtime(local_path)
                cloud_mtime = cloud_files[local_file]
                if local_mtime > cloud_mtime:
                    logger.info(f"Updating modified file: {local_file}")
                    connector.reload(local_path)
        # Обновляем список файлов в облаке после обновления
        cloud_files = connector.get_info()

        # 3. Удаляем из облака файлы, которых больше нет локально
        for cloud_file in list(cloud_files.keys()):
            if cloud_file not in local_files:
                logger.info(f"Deleting missing file from cloud: {cloud_file}")
                success = connector.delete(cloud_file)
                if success:
                    del cloud_files[cloud_file]

    except Exception as e:
        logger.error(f"Sync error: {e}")


def main():
    try:
        config = load_config()
        setup_logger(config["LOG_FILE_PATH"])

        logger.info(f"Starting sync service for folder: {config['LOCAL_FOLDER_PATH']}")

        connector = YandexDiskConnector(
            token=config["YANDEX_DISK_TOKEN"],
            cloud_folder=config["CLOUD_FOLDER_NAME"]
        )

        while True:
            sync_files(connector, config["LOCAL_FOLDER_PATH"], logger)
            time.sleep(int(config["SYNC_INTERVAL"]))
    except ValueError as e:
        print(f"Configuration error: {e}")
    except KeyboardInterrupt:
        logger.info("Sync service stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
