import os
import shutil
from pathlib import Path
from uuid import uuid4


BASE_STORAGE_PATH = Path(
    os.getenv("KELYVO_STORAGE_PATH", "kelyvo_storage")
)

BASE_STORAGE_PATH.mkdir(
    parents=True,
    exist_ok=True
)


class LocalStorageProvider:

    def save_file(self, file, folder="submissions"):
        folder_path = BASE_STORAGE_PATH / folder
        folder_path.mkdir(
            parents=True,
            exist_ok=True
        )

        extension = Path(file.filename).suffix

        stored_name = (
            f"{uuid4().hex}{extension}"
        )

        file_path = folder_path / stored_name

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(
                file.file,
                buffer
            )

        return {
            "filename": file.filename,
            "stored_name": stored_name,
            "path": str(file_path),
            "storage": "local"
        }


    def delete_file(self, path):

        file_path = Path(path)

        if file_path.exists():
            file_path.unlink()

        return True



storage_provider = LocalStorageProvider()